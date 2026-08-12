//! Integration tests for the `harvest schema` subcommand (issue #794).
//!
//! Black-box tests over the crate's public schema-gate surface: they write
//! contract files to a tempdir and drive the pure format/gate helpers and
//! `run_schema_check` / `run_schema_update` end to end. No database, no network.

use autumn_harvest_cli::{
    Cli, SchemaCheckFormat, format_schema_diff_text, run_schema_check, run_schema_update,
    schema_diff_json,
};
use clap::Parser;

const BASELINE: &str = r#"{
  "version": "0.0.0-test",
  "contract_version": "1",
  "workflows": [
    { "name": "onboarding",
      "input_schema": {
        "type": "object",
        "properties": { "user_id": {"type":"integer","format":"int64"},
                        "email": {"type":"string"} },
        "required": ["user_id","email"] } }
  ]
}"#;

/// `email` renamed to `email_address`.
const CURRENT_BREAKING: &str = r#"{
  "version": "0.0.0-test",
  "contract_version": "1",
  "workflows": [
    { "name": "onboarding",
      "input_schema": {
        "type": "object",
        "properties": { "user_id": {"type":"integer","format":"int64"},
                        "email_address": {"type":"string"} },
        "required": ["user_id","email_address"] } }
  ]
}"#;

/// A new optional field — safe on replay.
const CURRENT_COMPATIBLE: &str = r#"{
  "version": "0.0.0-test",
  "contract_version": "1",
  "workflows": [
    { "name": "onboarding",
      "input_schema": {
        "type": "object",
        "properties": { "user_id": {"type":"integer","format":"int64"},
                        "email": {"type":"string"},
                        "referral_code": {"type":["string","null"]} },
        "required": ["user_id","email"] } }
  ]
}"#;

fn tree(
    baseline: &str,
    current: &str,
) -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let b = dir.path().join("baseline.json");
    let c = dir.path().join("current.json");
    std::fs::write(&b, baseline).unwrap();
    std::fs::write(&c, current).unwrap();
    (dir, b, c)
}

// ── AC-4: exit-code contract ────────────────────────────────────────────────

#[test]
fn a_compatible_change_exits_zero() {
    let (_d, b, c) = tree(BASELINE, CURRENT_COMPATIBLE);
    let out = run_schema_check(&b, &c, SchemaCheckFormat::Text);
    assert!(out.is_ok(), "a compatible change must exit 0: {out:?}");
}

#[test]
fn a_breaking_change_exits_one() {
    let (_d, b, c) = tree(BASELINE, CURRENT_BREAKING);
    let err = run_schema_check(&b, &c, SchemaCheckFormat::Text)
        .expect_err("a breaking change must fail the gate");
    assert_eq!(
        err.exit_code(),
        1,
        "AC-4 mandates exit code 1 for a breaking delta"
    );
}

#[test]
fn a_missing_baseline_is_a_clear_error_not_a_silent_pass() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("nope.json");
    let c = dir.path().join("current.json");
    std::fs::write(&c, CURRENT_COMPATIBLE).unwrap();
    let err = run_schema_check(&missing, &c, SchemaCheckFormat::Text)
        .expect_err("a missing baseline must be an error, never a silent pass");
    assert!(
        format!("{err}").contains("nope.json"),
        "the error must name the missing path: {err}"
    );
}

#[test]
fn a_malformed_contract_is_a_clear_error() {
    let (_d, b, c) = tree("{ not json", CURRENT_COMPATIBLE);
    assert!(run_schema_check(&b, &c, SchemaCheckFormat::Text).is_err());
}

// ── AC-4: output formats ────────────────────────────────────────────────────

#[test]
fn text_output_uses_the_documented_workflow_field_verdict_reason_shape() {
    let baseline =
        autumn_harvest::schema_contract::WorkflowSchemaContract::parse(BASELINE).unwrap();
    let current =
        autumn_harvest::schema_contract::WorkflowSchemaContract::parse(CURRENT_BREAKING).unwrap();
    let diff = autumn_harvest::schema_contract::diff_schema_contracts(&baseline, &current);
    let text = format_schema_diff_text(&diff);

    assert!(
        text.contains("onboarding.input"),
        "text must be keyed by workflow + schema role: {text}"
    );
    assert!(
        text.contains("/email"),
        "text must name the offending field path: {text}"
    );
    assert!(
        text.contains("breaking"),
        "text must carry the verdict: {text}"
    );
    assert!(
        text.contains(" — "),
        "text must use the documented `<verdict> — <reason>` separator: {text}"
    );
}

#[test]
fn json_output_is_machine_readable_with_per_field_verdicts() {
    let baseline =
        autumn_harvest::schema_contract::WorkflowSchemaContract::parse(BASELINE).unwrap();
    let current =
        autumn_harvest::schema_contract::WorkflowSchemaContract::parse(CURRENT_BREAKING).unwrap();
    let diff = autumn_harvest::schema_contract::diff_schema_contracts(&baseline, &current);
    let text = schema_diff_json(&diff).expect("serialize");
    let v: serde_json::Value = serde_json::from_str(&text).expect("valid JSON");

    assert!(v["breaking_count"].as_u64().unwrap() > 0);
    let deltas = v["deltas"].as_array().expect("deltas array");
    let d = deltas
        .iter()
        .find(|d| d["verdict"] == "breaking")
        .expect("at least one breaking delta");
    for key in [
        "workflow",
        "role",
        "field_path",
        "change",
        "verdict",
        "reason",
    ] {
        assert!(
            d.get(key).is_some(),
            "machine-readable delta must carry `{key}`: {d}"
        );
    }
}

#[test]
fn a_clean_check_still_reports_the_summary() {
    let baseline =
        autumn_harvest::schema_contract::WorkflowSchemaContract::parse(BASELINE).unwrap();
    let diff = autumn_harvest::schema_contract::diff_schema_contracts(&baseline, &baseline);
    let text = format_schema_diff_text(&diff);
    assert!(
        text.contains("no breaking"),
        "a clean run must say so explicitly: {text}"
    );
}

// ── AC-5: the escape hatch ──────────────────────────────────────────────────

#[test]
fn update_refuses_a_breaking_change_without_an_acknowledgement() {
    let (_d, b, c) = tree(BASELINE, CURRENT_BREAKING);
    let err = run_schema_update(&b, &c, None, None)
        .expect_err("regenerating over a breaking delta must require --acknowledge");
    assert!(
        format!("{err}").contains("acknowledge"),
        "the error must tell the author exactly how to proceed: {err}"
    );
    // The baseline must be left untouched on refusal.
    assert_eq!(std::fs::read_to_string(&b).unwrap(), BASELINE);
}

#[test]
fn update_with_an_acknowledgement_writes_the_baseline_and_records_the_reason() {
    let (_d, b, c) = tree(BASELINE, CURRENT_BREAKING);
    run_schema_update(
        &b,
        &c,
        Some("renamed for GDPR; in-flight runs drained via reset (#148)"),
        Some("docs/changelog.d/pr-794-schema-gate.md"),
    )
    .expect("acknowledged update must succeed");

    // The rewritten baseline now matches current, so `check` is clean...
    assert!(run_schema_check(&b, &c, SchemaCheckFormat::Text).is_ok());

    // ...and the justification is a visible, permanent record in the artifact.
    let written = std::fs::read_to_string(&b).unwrap();
    assert!(
        written.contains("GDPR"),
        "the acknowledgement reason must be visible in the checked-in diff: {written}"
    );
    assert!(written.contains("acknowledged_breaking_changes"));
    assert!(written.contains("pr-794-schema-gate.md"));
}

#[test]
fn update_of_a_compatible_change_needs_no_acknowledgement() {
    let (_d, b, c) = tree(BASELINE, CURRENT_COMPATIBLE);
    run_schema_update(&b, &c, None, None).expect("a compatible update needs no acknowledgement");
    assert!(run_schema_check(&b, &c, SchemaCheckFormat::Text).is_ok());
    let written = std::fs::read_to_string(&b).unwrap();
    assert!(written.contains("referral_code"));
}

#[test]
fn update_refuses_a_blank_acknowledgement() {
    let (_d, b, c) = tree(BASELINE, CURRENT_BREAKING);
    assert!(
        run_schema_update(&b, &c, Some("   "), None).is_err(),
        "a whitespace-only justification is a rubber stamp and must be refused"
    );
}

// ── clap wiring ─────────────────────────────────────────────────────────────

#[test]
fn schema_check_parses_with_defaults() {
    let cli = Cli::try_parse_from([
        "harvest",
        "schema",
        "check",
        "--baseline",
        "docs/workflow-schema-contract.json",
        "--current",
        "cur.json",
    ])
    .expect("`harvest schema check` must parse");
    // The command must be recognised as local (no --url required).
    let _ = cli;
}

#[test]
fn schema_check_accepts_json_format() {
    Cli::try_parse_from([
        "harvest",
        "schema",
        "check",
        "--current",
        "cur.json",
        "--format",
        "json",
    ])
    .expect("--format json must parse");
}

#[test]
fn schema_check_baseline_defaults_to_the_documented_path() {
    Cli::try_parse_from(["harvest", "schema", "check", "--current", "cur.json"])
        .expect("--baseline must have a default so the one-liner is short");
}

#[test]
fn schema_update_parses_with_acknowledge() {
    Cli::try_parse_from([
        "harvest",
        "schema",
        "update",
        "--current",
        "cur.json",
        "--acknowledge",
        "deliberate migration",
        "--recorded-in",
        "docs/changelog.d/pr-794.md",
    ])
    .expect("`harvest schema update --acknowledge` must parse");
}

// ── The documented one-liner: a bare `/workflows/registered` body works ─────

#[test]
fn current_may_be_a_raw_registered_workflows_response() {
    let dir = tempfile::tempdir().unwrap();
    let b = dir.path().join("baseline.json");
    let c = dir.path().join("current.json");
    std::fs::write(&b, BASELINE).unwrap();
    // Exactly what `curl .../workflows/registered` returns.
    std::fs::write(
        &c,
        r#"[
          { "name":"onboarding",
            "input_schema":{"type":"object",
              "properties":{"user_id":{"type":"integer","format":"int64"},
                            "email":{"type":"string"}},
              "required":["user_id","email"]},
            "output_schema":null, "error_schema":null, "mcp":false }
        ]"#,
    )
    .unwrap();
    run_schema_check(&b, &c, SchemaCheckFormat::Text)
        .expect("a bare GET /workflows/registered body must be usable as --current");
}

// ── review hardening ────────────────────────────────────────────────────────

/// End-to-end through the **real binary**: parse the flag, run the gate, print
/// the diff, exit non-zero.
///
/// The in-process helpers cannot see `println!`, so a `--format json` that
/// silently rendered text — or a flag that never reached the renderer — would
/// pass every in-process test. Spawning the binary is the only way to assert
/// what CI actually consumes.
#[test]
fn the_real_binary_emits_json_and_exits_one_on_a_breaking_change() {
    let (_d, b, c) = tree(BASELINE, CURRENT_BREAKING);
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_harvest"))
        .args([
            "schema",
            "check",
            "--baseline",
            b.to_str().unwrap(),
            "--current",
            c.to_str().unwrap(),
            "--format",
            "json",
        ])
        .output()
        .expect("the harvest binary must run");

    assert_eq!(out.status.code(), Some(1), "AC-4: breaking exits 1");

    let stdout = String::from_utf8(out.stdout).expect("utf-8 stdout");
    let v: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|e| {
        panic!("`--format json` must emit parseable JSON, got:\n{stdout}\n{e}")
    });
    assert!(v["breaking_count"].as_u64().unwrap() > 0);
    assert!(v["deltas"].is_array());
}

/// The same run in the default format is human-readable lines, not JSON —
/// proving the flag actually selects a renderer.
#[test]
fn the_real_binary_default_format_is_text_not_json() {
    let (_d, b, c) = tree(BASELINE, CURRENT_BREAKING);
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_harvest"))
        .args([
            "schema",
            "check",
            "--baseline",
            b.to_str().unwrap(),
            "--current",
            c.to_str().unwrap(),
        ])
        .output()
        .expect("the harvest binary must run");

    assert_eq!(out.status.code(), Some(1));
    let stdout = String::from_utf8(out.stdout).expect("utf-8 stdout");
    assert!(
        serde_json::from_str::<serde_json::Value>(stdout.trim()).is_err(),
        "the default format must NOT be JSON: {stdout}"
    );
    assert!(
        stdout.contains("onboarding.input"),
        "text output is `workflow.role: …`: {stdout}"
    );
}

/// A clean run exits 0 through the real binary too — the other half of AC-4.
#[test]
fn the_real_binary_exits_zero_on_a_compatible_change() {
    let (_d, b, c) = tree(BASELINE, CURRENT_COMPATIBLE);
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_harvest"))
        .args([
            "schema",
            "check",
            "--baseline",
            b.to_str().unwrap(),
            "--current",
            c.to_str().unwrap(),
        ])
        .output()
        .expect("the harvest binary must run");
    assert_eq!(out.status.code(), Some(0), "compatible must exit 0");
}

/// The JSON payload is the machine-readable contract from AC-4: it must parse,
/// and carry the fields a CI consumer keys on.
#[test]
fn the_json_diff_carries_the_documented_fields() {
    let base = autumn_harvest::WorkflowSchemaContract::parse(BASELINE).expect("baseline");
    let cur = autumn_harvest::WorkflowSchemaContract::parse(CURRENT_BREAKING).expect("current");
    let diff = autumn_harvest::diff_schema_contracts(&base, &cur);

    let rendered = schema_diff_json(&diff).expect("serialize");
    let v: serde_json::Value = serde_json::from_str(&rendered).expect("emitted JSON must parse");

    assert!(v["breaking_count"].as_u64().unwrap() > 0);
    assert!(
        v["compatible_count"].is_number(),
        "documented field missing"
    );
    let d = &v["deltas"][0];
    for field in [
        "workflow",
        "role",
        "field_path",
        "change",
        "verdict",
        "reason",
    ] {
        assert!(!d[field].is_null(), "delta must carry `{field}`: {d}");
    }
}

/// A truncated report cannot certify that nothing is breaking, so the gate must
/// fail closed rather than exit 0 on a partial answer.
///
/// The fixture produces **only compatible** deltas past the cap: with breaking
/// ones the ordinary `has_breaking()` check would fail it anyway, and the
/// truncation guard would never be the reason.
#[test]
fn a_truncated_but_entirely_compatible_diff_still_fails_closed() {
    let n = autumn_harvest::MAX_DELTAS + 10;
    // Adding optional properties is compatible, so every delta is compatible.
    let props: serde_json::Map<String, serde_json::Value> = (0..n)
        .map(|i| {
            (
                format!("f{i}"),
                serde_json::json!({"type": ["string", "null"]}),
            )
        })
        .collect();
    let baseline = r#"{
      "version": "0.0.0-test",
      "contract_version": "1",
      "workflows": [{ "name": "wf", "input_schema": {"type":"object","properties":{}} }]
    }"#;
    let current = serde_json::json!({
        "version": "0.0.0-test",
        "contract_version": "1",
        "workflows": [{
            "name": "wf",
            "input_schema": {"type": "object", "properties": props},
        }],
    })
    .to_string();

    let (_d, b, c) = tree(baseline, &current);

    // Precondition: the diff really is compatible-only and truncated.
    let base = autumn_harvest::WorkflowSchemaContract::parse(baseline).unwrap();
    let cur = autumn_harvest::WorkflowSchemaContract::parse(&current).unwrap();
    let diff = autumn_harvest::diff_schema_contracts(&base, &cur);
    assert!(!diff.has_breaking(), "fixture must be compatible-only");
    assert!(diff.truncated, "fixture must exceed the delta cap");

    let err = run_schema_check(&b, &c, SchemaCheckFormat::Text)
        .expect_err("an incomplete report must never pass the gate");
    assert_eq!(err.exit_code(), 1);
    assert!(
        format!("{err}").contains("truncated"),
        "the error must say why it refused: {err}"
    );
}

/// `update` replaces the baseline via a sibling temp file + `rename`, so a
/// reader never observes a half-written contract.
///
/// What this asserts is the observable half: after a successful update the
/// baseline parses **and** no `.tmp-*` sibling is left behind — which fails if
/// the rename is dropped or the temp file leaks.
///
/// Crash-atomicity itself (dying mid-write) is deliberately not asserted: it
/// needs the process to be killed between the write and the rename, which an
/// in-process test cannot arrange. A test shaped like one would pass whatever
/// the implementation did, so it is not written.
#[test]
fn update_leaves_a_parseable_baseline_and_no_temp_files() {
    let (dir, b, c) = tree(BASELINE, CURRENT_COMPATIBLE);
    run_schema_update(&b, &c, None, None).expect("a compatible update succeeds");

    let written = std::fs::read_to_string(&b).expect("baseline still readable");
    autumn_harvest::WorkflowSchemaContract::parse(&written)
        .expect("the rewritten baseline must be a valid contract");

    let strays: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.contains(".tmp-"))
        .collect();
    assert!(
        strays.is_empty(),
        "atomic write left temp files: {strays:?}"
    );
}

/// A refused update must leave the baseline byte-for-byte untouched — a gate
/// that half-writes on refusal is worse than no gate.
#[test]
fn a_refused_update_does_not_modify_the_baseline() {
    let (_d, b, c) = tree(BASELINE, CURRENT_BREAKING);
    let before = std::fs::read_to_string(&b).unwrap();
    run_schema_update(&b, &c, None, None).expect_err("breaking without --acknowledge is refused");
    assert_eq!(
        std::fs::read_to_string(&b).unwrap(),
        before,
        "the baseline must be untouched after a refusal"
    );
}

// ── review round: an empty `--current` is a broken producer, not a deletion ──

const EMPTY_CONTRACT: &str = r#"{
  "version": "0.0.0-test",
  "contract_version": "1",
  "workflows": []
}"#;

/// A dump that published nothing must be diagnosed, not diffed.
///
/// Diffed literally it is "every workflow removed" — technically true, but it
/// buries the real cause (a producer that registered no workflows) under N
/// breaking deltas and invites `--acknowledge`, which would overwrite the
/// baseline with nothing and disarm the gate permanently.
#[test]
fn an_empty_current_is_diagnosed_rather_than_read_as_a_mass_deletion() {
    let (_d, b, c) = tree(BASELINE, EMPTY_CONTRACT);
    let err = run_schema_check(&b, &c, SchemaCheckFormat::Text)
        .expect_err("an empty current contract must be refused");
    let msg = err.to_string();
    assert!(
        msg.contains("publishes no workflows"),
        "the refusal must name the real cause: {msg}"
    );
    assert!(
        msg.contains("producer"),
        "it must point the author at the producer, not at the schemas: {msg}"
    );
    // Distinct from the ordinary breaking-change failure: without the guard
    // this is `SchemaContractBreaking`, whose message says none of the above.
    assert!(
        !matches!(
            err,
            autumn_harvest_cli::CliError::SchemaContractBreaking { .. }
        ),
        "an empty dump must not be reported as an ordinary breaking change: {err:?}"
    );
}

/// `update` must refuse it too — that is the path that would overwrite the
/// baseline with the empty document and disarm every later run.
#[test]
fn an_empty_current_cannot_overwrite_the_baseline_even_with_an_acknowledgement() {
    let (_d, b, c) = tree(BASELINE, EMPTY_CONTRACT);
    let before = std::fs::read_to_string(&b).unwrap();
    let err = run_schema_update(&b, &c, Some("drained"), None)
        .expect_err("an empty current contract must be refused by update too");
    assert!(err.to_string().contains("publishes no workflows"), "{err}");
    assert_eq!(
        std::fs::read_to_string(&b).unwrap(),
        before,
        "the baseline must be left untouched"
    );
}

/// The guard is not a blanket refusal of empty documents: an empty baseline
/// (the very first run, before anything publishes a schema) still works.
#[test]
fn an_empty_baseline_against_an_empty_current_is_not_refused() {
    let (_d, b, c) = tree(EMPTY_CONTRACT, EMPTY_CONTRACT);
    run_schema_check(&b, &c, SchemaCheckFormat::Text).expect("empty-to-empty is a no-op");
}

/// ...and adding the first workflow to an empty baseline is compatible.
#[test]
fn growing_from_an_empty_baseline_is_compatible() {
    let (_d, b, c) = tree(EMPTY_CONTRACT, BASELINE);
    run_schema_check(&b, &c, SchemaCheckFormat::Text)
        .expect("publishing a new workflow must not fail the gate");
}
