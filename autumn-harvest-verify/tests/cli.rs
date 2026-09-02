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
    assert!(!report.rustc_version.is_empty());
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
    assert!(!stderr(&out).is_empty());
}
