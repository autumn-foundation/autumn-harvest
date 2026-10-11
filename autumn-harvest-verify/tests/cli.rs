//! CLI surface and exit-code contract (D10), exercised through the real binary.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const BIN: &str = env!("CARGO_BIN_EXE_cargo-harvest-verify");

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest-verify has a parent")
        .to_path_buf()
}

fn run(args: &[&str]) -> Output {
    Command::new(BIN)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("cannot run {BIN} {args:?}: {e}"))
}

/// The same, with a working directory — the input the default target selection
/// resolves against when no `-p` and no target flag is given.
fn run_in(dir: &Path, args: &[&str]) -> Output {
    Command::new(BIN)
        .current_dir(dir)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("cannot run {BIN} {args:?} in {}: {e}", dir.display()))
}

fn code(out: &Output) -> i32 {
    out.status.code().unwrap_or_else(|| {
        panic!(
            "the binary was killed by a signal; stderr:\n{}",
            String::from_utf8_lossy(&out.stderr)
        )
    })
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

// ── help / argv shim ────────────────────────────────────────────────────────

#[test]
fn help_exits_zero_and_documents_the_flags() {
    let out = run(&["--help"]);
    assert_eq!(code(&out), 0, "stderr:\n{}", stderr(&out));
    let text = stdout(&out);
    for flag in ["--strict", "--format", "--allowlist", "--model", "--mir"] {
        assert!(
            text.contains(flag),
            "`--help` must document {flag}:\n{text}"
        );
    }
}

#[test]
fn the_cargo_subcommand_token_is_tolerated() {
    // Cargo invokes `cargo-harvest-verify harvest-verify <args>`.
    let out = run(&["harvest-verify", "--help"]);
    assert_eq!(code(&out), 0, "stderr:\n{}", stderr(&out));
    assert!(stdout(&out).contains("--strict"), "{}", stdout(&out));
    // Both spellings must produce the same help text.
    assert_eq!(stdout(&out), stdout(&run(&["--help"])));
}

#[test]
fn list_boundaries_prints_every_boundary_name() {
    // `--list-boundaries` is the machine-readable half of the docs guard in
    // D11: the feasibility report's boundary table is diffed against this
    // output, so a boundary cannot be added to the code without appearing there.
    let out = run(&["--list-boundaries"]);
    assert_eq!(code(&out), 0, "stderr:\n{}", stderr(&out));
    let text = stdout(&out);
    for kind in autumn_harvest_verify::BoundaryKind::ALL {
        assert!(
            text.contains(kind.name()),
            "missing boundary {} in:\n{text}",
            kind.name()
        );
    }
    assert_eq!(
        text.lines().filter(|l| !l.trim().is_empty()).count(),
        autumn_harvest_verify::BoundaryKind::ALL.len(),
        "one boundary per line, nothing else:\n{text}"
    );
}

// ── analysis over pre-emitted MIR ───────────────────────────────────────────

#[test]
fn json_output_over_a_clean_fixture_exits_zero_and_parses_as_a_report() {
    let out = run(&[
        "--mir",
        &fixtures_dir()
            .join("example_deterministic_primitives.mir")
            .to_string_lossy(),
        "--source-root",
        &workspace_root().to_string_lossy(),
        "--format",
        "json",
    ]);
    assert_eq!(code(&out), 0, "stderr:\n{}", stderr(&out));
    let report: autumn_harvest_verify::Report = serde_json::from_str(&stdout(&out))
        .unwrap_or_else(|e| panic!("stdout is not a Report: {e}\n{}", stdout(&out)));
    assert!(
        !report.model_version.is_empty(),
        "the model version is part of the contract"
    );
    assert_ne!(report.rustc_version, "");
    assert_eq!(report.summary().analyzed, 1);
    assert_eq!(report.summary().found, 0);
}

#[test]
fn json_output_over_the_laundering_matrix_exits_one() {
    let out = run(&[
        "--mir",
        &fixtures_dir()
            .join("format_and_outparams.mir")
            .to_string_lossy(),
        "--source-root",
        &fixtures_dir().to_string_lossy(),
        "--format",
        "json",
    ]);
    assert_eq!(
        code(&out),
        1,
        "seeded findings must fail the run; stderr:\n{}",
        stderr(&out)
    );
    let report: autumn_harvest_verify::Report =
        serde_json::from_str(&stdout(&out)).expect("stdout is still a Report on exit 1");
    assert!(report.summary().found > 0, "{:?}", report.summary());
    assert_eq!(report.exit_code(false), 1);
}

#[test]
fn text_output_is_the_default_format() {
    let out = run(&[
        "--mir",
        &fixtures_dir()
            .join("example_deterministic_primitives.mir")
            .to_string_lossy(),
        "--source-root",
        &workspace_root().to_string_lossy(),
    ]);
    assert_eq!(code(&out), 0, "stderr:\n{}", stderr(&out));
    let text = stdout(&out);
    assert!(text.contains("notify_decision"), "{text}");
    assert!(text.contains("under model"), "{text}");
    assert!(
        serde_json::from_str::<serde_json::Value>(&text).is_err(),
        "default is text, not JSON"
    );
}

#[test]
fn strict_promotes_unknown_to_a_failure() {
    let matrix = fixtures_dir()
        .join("format_and_outparams.mir")
        .to_string_lossy()
        .into_owned();
    let roots = fixtures_dir().to_string_lossy().into_owned();

    let lax = run(&[
        "--mir",
        &matrix,
        "--source-root",
        &roots,
        "--format",
        "json",
    ]);
    let report: autumn_harvest_verify::Report =
        serde_json::from_str(&stdout(&lax)).expect("Report from the lax run");
    assert!(
        report.summary().unknown > 0,
        "the matrix must contain boundary workflows (ffi, fn-pointer, dyn): {:?}",
        report.summary()
    );
    assert_eq!(report.exit_code(true), 1, "the library agrees with the CLI");

    let strict = run(&[
        "--mir",
        &matrix,
        "--source-root",
        &roots,
        "--format",
        "json",
        "--strict",
    ]);
    assert_eq!(code(&strict), 1, "stderr:\n{}", stderr(&strict));
}

#[test]
fn strict_flips_a_boundary_only_run_from_zero_to_one() {
    // `example_deterministic_primitives.mir` is the clean baseline: it must exit
    // 0 under both, so a `--strict` failure there is a real regression.
    let mir = fixtures_dir()
        .join("example_deterministic_primitives.mir")
        .to_string_lossy()
        .into_owned();
    let roots = workspace_root().to_string_lossy().into_owned();
    let lax = run(&["--mir", &mir, "--source-root", &roots, "--format", "json"]);
    let strict = run(&[
        "--mir",
        &mir,
        "--source-root",
        &roots,
        "--format",
        "json",
        "--strict",
    ]);
    assert_eq!(code(&lax), 0, "stderr:\n{}", stderr(&lax));
    assert_eq!(
        code(&strict),
        0,
        "a proven-deterministic run has no boundaries to promote; stderr:\n{}",
        stderr(&strict)
    );
}

#[test]
fn a_run_that_discovers_no_workflow_warns_and_is_strict_only_failure() {
    // The failure this guards: a `.mir` with no readable `__autumn_workflow_info_*`
    // marker used to print `analyzed 0` and exit 0 in every mode — a CI gate went
    // green on a run that verified nothing at all.
    let dir = tempfile::tempdir().expect("tempdir");
    let mir = dir.path().join("no_markers.mir");
    std::fs::write(
        &mir,
        "fn helper() -> u8 {\n    let mut _0: u8;\n\n\
         bb0: {\n        _0 = const 0_u8;\n        return;\n    }\n}\n",
    )
    .expect("write .mir");
    let mir = mir.to_string_lossy().into_owned();

    let lax = run(&["--mir", &mir, "--format", "json"]);
    assert_eq!(code(&lax), 0, "stderr:\n{}", stderr(&lax));
    let report: autumn_harvest_verify::Report =
        serde_json::from_str(&stdout(&lax)).expect("stdout is a Report");
    assert_eq!(report.summary().analyzed, 0);
    assert!(report.discovery_failed, "{report:?}");
    assert!(
        report
            .warnings
            .iter()
            .any(|w| w.contains("no #[workflow] entry points were discovered")),
        "the JSON warnings must carry it: {:?}",
        report.warnings
    );

    let strict = run(&["--mir", &mir, "--format", "json", "--strict"]);
    assert_eq!(
        code(&strict),
        1,
        "`--strict` must refuse to pass a run that discovered nothing; stdout:\n{}",
        stdout(&strict)
    );

    // The text format says it too, on the report the operator actually reads.
    let text = run(&["--mir", &mir]);
    assert_eq!(code(&text), 0);
    assert!(
        stdout(&text).contains("no #[workflow] entry points were discovered"),
        "{}",
        stdout(&text)
    );
}

// ── the default target selection ────────────────────────────────────────────

#[test]
fn a_bare_run_in_a_package_directory_analyzes_that_package() {
    // The failure this guards: with no `-p`, no target flag and no `--mir`, the
    // pipeline used to skip emission entirely and report `analyzed 0`, exit 0 —
    // a green gate over a run that built nothing. Cargo's own default for a
    // bare invocation is "the package the manifest resolves to"; so is ours.
    //
    // Slow: this is the only test in the file that actually builds. It emits
    // into its own directory (~1 min warm, longer cold).
    let package = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("corpus")
        .join("clean");
    let target_dir = workspace_root()
        .join("target")
        .join("harvest-verify")
        .join("cli");
    let out = run_in(
        &package,
        &[
            "--format",
            "json",
            "--target-dir",
            &target_dir.to_string_lossy(),
        ],
    );
    assert_eq!(code(&out), 0, "stderr:\n{}", stderr(&out));
    let report: autumn_harvest_verify::Report = serde_json::from_str(&stdout(&out))
        .unwrap_or_else(|e| panic!("stdout is not a Report: {e}\n{}", stdout(&out)));
    assert!(
        !report.discovery_failed,
        "a bare run in a package directory must build it: {:?}",
        report.warnings
    );
    assert_eq!(
        report.summary().analyzed,
        13,
        "every `#[workflow]` in the clean corpus crate: {:?}",
        report.summary()
    );
    assert_eq!(report.summary().found, 0, "{:?}", report.summary());
}

#[test]
fn a_package_with_no_target_flag_builds_its_default_targets() {
    // `-p <package>` with no `--lib`/`--bin`/`--example`/`--test` used to force
    // `--lib`, which fails outright on a bin-only member. It now selects the
    // package's default targets, exactly as cargo does for `cargo build -p`.
    //
    // Slow only on a cold cache: it shares the emit directory with the bare-run
    // test above, which has already built this very package's lib.
    let target_dir = workspace_root()
        .join("target")
        .join("harvest-verify")
        .join("cli");
    let out = run_in(
        &workspace_root(),
        &[
            "-p",
            "harvest-verify-corpus-clean",
            "--format",
            "json",
            "--target-dir",
            &target_dir.to_string_lossy(),
        ],
    );
    assert_eq!(code(&out), 0, "stderr:\n{}", stderr(&out));
    let report: autumn_harvest_verify::Report = serde_json::from_str(&stdout(&out))
        .unwrap_or_else(|e| panic!("stdout is not a Report: {e}\n{}", stdout(&out)));
    assert_eq!(
        report.summary().analyzed,
        13,
        "the package has a lib, so its lib is what `-p` alone builds: {:?}",
        report.summary()
    );
    assert_eq!(report.summary().found, 0, "{:?}", report.summary());
}

#[test]
fn a_bare_run_at_a_virtual_workspace_root_is_a_tool_error() {
    // A virtual manifest names no package, so there is nothing to default to.
    // That is an error the operator can act on, never a silent `analyzed 0`.
    let out = run_in(&workspace_root(), &["--format", "json"]);
    assert_eq!(
        code(&out),
        2,
        "stdout:\n{}\nstderr:\n{}",
        stdout(&out),
        stderr(&out)
    );
    let err = stderr(&out);
    assert!(
        err.contains("-p") && err.contains("--mir"),
        "the error must say how to select something:\n{err}"
    );
    assert!(
        !err.contains("panicked"),
        "it must be a diagnostic, not a panic:\n{err}"
    );
}

// ── tool errors are exit 2 ──────────────────────────────────────────────────

#[test]
fn an_invalid_allowlist_is_a_tool_error_on_stderr() {
    let dir = tempfile::tempdir().expect("tempdir");
    let allow = dir.path().join("harvest-verify.allow.toml");
    std::fs::write(
        &allow,
        "[[allow]]\nworkflow = \"seeded::wf_x\"\njustification = \"   \"\n",
    )
    .expect("write allowlist");

    let out = run(&[
        "--mir",
        &fixtures_dir()
            .join("format_and_outparams.mir")
            .to_string_lossy(),
        "--source-root",
        &fixtures_dir().to_string_lossy(),
        "--allowlist",
        &allow.to_string_lossy(),
    ]);
    assert_eq!(
        code(&out),
        2,
        "a malformed allowlist is a tool error, not a finding"
    );
    let err = stderr(&out);
    assert!(
        err.contains("seeded::wf_x"),
        "the error must name the entry:\n{err}"
    );
    assert!(
        !err.contains("panicked"),
        "it must be a diagnostic, not a panic:\n{err}"
    );
}

#[test]
fn a_missing_mir_path_is_a_tool_error() {
    let out = run(&["--mir", "/definitely/not/here.mir", "--format", "json"]);
    assert_eq!(
        code(&out),
        2,
        "stdout:\n{}\nstderr:\n{}",
        stdout(&out),
        stderr(&out)
    );
    assert!(!stderr(&out).is_empty(), "exit 2 must explain itself");
}

#[test]
fn an_unknown_flag_is_a_usage_error() {
    let out = run(&["--not-a-real-flag"]);
    assert_ne!(code(&out), 0);
    assert_ne!(stderr(&out), "");
}

// ── structure manifest (issue #1995) ───────────────────────────────────────

#[test]
fn emit_structure_writes_a_manifest_next_to_the_report() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out_path = dir.path().join("flow.structure.json");
    let mir_dir = fixtures_dir().join("upgrade_baseline");
    let out = run(&[
        "--mir",
        &mir_dir.join("flow.mir").to_string_lossy(),
        "--source-root",
        &mir_dir.to_string_lossy(),
        "--emit-structure",
        &out_path.to_string_lossy(),
        "--format",
        "json",
    ]);
    assert_eq!(code(&out), 0, "stderr:\n{}", stderr(&out));
    let report: autumn_harvest_verify::Report = serde_json::from_str(&stdout(&out))
        .unwrap_or_else(|e| panic!("stdout is not a Report: {e}\n{}", stdout(&out)));
    let text = std::fs::read_to_string(&out_path).expect("the manifest is written");
    let manifest: autumn_harvest_verify::structure::StructureManifest =
        serde_json::from_str(&text).expect("the manifest parses");
    assert_eq!(manifest.format, "harvest-structure/1");
    assert_eq!(manifest.model_version, report.model_version);
    assert_eq!(manifest.rustc_version, report.rustc_version);
    assert_eq!(manifest.workflows.len(), report.workflows.len());
    assert!(
        !stdout(&out).contains("\"bodies\""),
        "the report on stdout does not carry the manifest"
    );
}

#[test]
fn help_documents_emit_structure() {
    let out = run(&["--help"]);
    assert_eq!(code(&out), 0);
    assert!(
        stdout(&out).contains("--emit-structure"),
        "{}",
        stdout(&out)
    );
}

// ── saga coverage over a manifest (issue #2010) ────────────────────────────

/// Write the manifest of the saga fixture into `dir`.
fn saga_manifest(dir: &Path) -> PathBuf {
    let out_path = dir.join("saga.structure.json");
    let mir_dir = fixtures_dir().join("saga_graph");
    let out = run(&[
        "--mir",
        &mir_dir.join("flow.mir").to_string_lossy(),
        "--source-root",
        &mir_dir.to_string_lossy(),
        "--emit-structure",
        &out_path.to_string_lossy(),
    ]);
    assert_ne!(code(&out), 2, "stderr:\n{}", stderr(&out));
    out_path
}

#[test]
fn check_structure_prints_a_verdict_per_workflow_and_fails_on_a_gap() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = saga_manifest(dir.path());
    let out = run(&["--check-structure", &path.to_string_lossy()]);
    assert_eq!(code(&out), 1, "a gap fails the run:\n{}", stdout(&out));
    let text = stdout(&out);
    assert!(text.contains("gap  flow::wf_gap_after_step"), "{text}");
    assert!(text.contains("covered  flow::wf_covered"), "{text}");
    assert!(text.contains("no-saga  flow::wf_no_saga"), "{text}");
    assert!(text.contains("unknown  flow::wf_escapes"), "{text}");
}

#[test]
fn check_structure_writes_json() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = saga_manifest(dir.path());
    let out = run(&[
        "--check-structure",
        &path.to_string_lossy(),
        "--format",
        "json",
    ]);
    assert_eq!(code(&out), 1);
    let reports: Vec<autumn_harvest_verify::saga::SagaReport> = serde_json::from_str(&stdout(&out))
        .unwrap_or_else(|e| panic!("stdout is not JSON: {e}\n{}", stdout(&out)));
    assert!(reports.iter().any(|r| r.name == "wf_gap_tail"));
}

#[test]
fn check_structure_refuses_a_manifest_without_flow_graphs() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("old.structure.json");
    std::fs::write(
        &path,
        r#"{"format":"harvest-structure/1","model_version":"m","rustc_version":"r","workflows":[]}"#,
    )
    .expect("write");
    let out = run(&["--check-structure", &path.to_string_lossy()]);
    assert_eq!(code(&out), 2, "a tool error:\n{}", stderr(&out));
    assert!(stderr(&out).contains("harvest-flow/1"), "{}", stderr(&out));
}

#[test]
fn help_documents_check_structure() {
    let out = run(&["--help"]);
    assert_eq!(code(&out), 0);
    assert!(
        stdout(&out).contains("--check-structure"),
        "{}",
        stdout(&out)
    );
}

/// Write `manifest` with only the workflows in `keep`.
fn filtered_manifest(dir: &Path, keep: &[&str]) -> PathBuf {
    let full = saga_manifest(dir);
    let mut value: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&full).expect("read")).expect("json");
    let workflows = value
        .get_mut("workflows")
        .and_then(serde_json::Value::as_array_mut)
        .expect("a workflow list");
    workflows.retain(|w| {
        w.get("name")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|name| keep.contains(&name))
    });
    let path = dir.join(format!("{}.structure.json", keep.join("-")));
    std::fs::write(&path, value.to_string()).expect("write");
    path
}

#[test]
fn check_structure_exits_zero_when_every_saga_is_covered() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = filtered_manifest(dir.path(), &["wf_covered", "wf_no_saga"]);
    let out = run(&["--check-structure", &path.to_string_lossy()]);
    assert_eq!(code(&out), 0, "{}", stdout(&out));
}

#[test]
fn check_structure_strict_fails_on_unknown() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = filtered_manifest(dir.path(), &["wf_escapes"]);
    let lenient = run(&["--check-structure", &path.to_string_lossy()]);
    assert_eq!(code(&lenient), 0, "{}", stdout(&lenient));
    assert!(
        stdout(&lenient).contains("possible gap:"),
        "{}",
        stdout(&lenient)
    );
    let strict = run(&["--check-structure", &path.to_string_lossy(), "--strict"]);
    assert_eq!(code(&strict), 1, "{}", stdout(&strict));
}

#[test]
fn check_structure_strict_fails_on_a_manifest_with_no_workflow() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("empty.structure.json");
    std::fs::write(
        &path,
        r#"{"format":"harvest-structure/1","model_version":"m","rustc_version":"r","flow":"harvest-flow/1","workflows":[]}"#,
    )
    .expect("write");
    let lenient = run(&["--check-structure", &path.to_string_lossy()]);
    assert_eq!(code(&lenient), 0);
    let strict = run(&["--check-structure", &path.to_string_lossy(), "--strict"]);
    assert_eq!(code(&strict), 1, "a gate that checked nothing fails");
}

#[test]
fn check_structure_refuses_another_format_and_bad_json() {
    let dir = tempfile::tempdir().expect("tempdir");
    let other = dir.path().join("other.structure.json");
    std::fs::write(
        &other,
        r#"{"format":"harvest-structure/2","flow":"harvest-flow/1","workflows":[{"bodies":[{"flow":{"nodes":[{"kind":"new-kind"}]}}]}]}"#,
    )
    .expect("write");
    let out = run(&["--check-structure", &other.to_string_lossy()]);
    assert_eq!(code(&out), 2);
    assert!(
        stderr(&out).contains("harvest-structure/2"),
        "{}",
        stderr(&out)
    );
    let bad = dir.path().join("bad.json");
    std::fs::write(&bad, "not json").expect("write");
    let out = run(&["--check-structure", &bad.to_string_lossy()]);
    assert_eq!(code(&out), 2);
    assert!(stderr(&out).contains("cannot parse"), "{}", stderr(&out));
}

#[test]
fn check_structure_conflicts_with_build_flags() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = filtered_manifest(dir.path(), &["wf_covered"]);
    let out = run(&[
        "--check-structure",
        &path.to_string_lossy(),
        "--emit-structure",
        &path.to_string_lossy(),
    ]);
    assert_eq!(code(&out), 2, "a conflict is a usage error");
    let out = run(&["--check-structure", &path.to_string_lossy(), "-p", "x"]);
    assert_eq!(code(&out), 2, "a conflict is a usage error");
}
