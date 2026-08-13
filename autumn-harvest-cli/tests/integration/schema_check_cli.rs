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
    let out = run_schema_check(&b, &c, SchemaCheckFormat::Text, false, None);
    assert!(out.is_ok(), "a compatible change must exit 0: {out:?}");
}

#[test]
fn a_breaking_change_exits_one() {
    let (_d, b, c) = tree(BASELINE, CURRENT_BREAKING);
    let err = run_schema_check(&b, &c, SchemaCheckFormat::Text, false, None)
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
    let err = run_schema_check(&missing, &c, SchemaCheckFormat::Text, false, None)
        .expect_err("a missing baseline must be an error, never a silent pass");
    assert!(
        format!("{err}").contains("nope.json"),
        "the error must name the missing path: {err}"
    );
}

#[test]
fn a_malformed_contract_is_a_clear_error() {
    let (_d, b, c) = tree("{ not json", CURRENT_COMPATIBLE);
    assert!(run_schema_check(&b, &c, SchemaCheckFormat::Text, false, None).is_err());
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
    assert!(run_schema_check(&b, &c, SchemaCheckFormat::Text, false, None).is_ok());

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
    assert!(run_schema_check(&b, &c, SchemaCheckFormat::Text, false, None).is_ok());
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
    run_schema_check(&b, &c, SchemaCheckFormat::Text, false, None)
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

    let err = run_schema_check(&b, &c, SchemaCheckFormat::Text, false, None)
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
    let err = run_schema_check(&b, &c, SchemaCheckFormat::Text, false, None)
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
    run_schema_check(&b, &c, SchemaCheckFormat::Text, false, None)
        .expect("empty-to-empty is a no-op");
}

/// ...and adding the first workflow to an empty baseline is compatible.
#[test]
fn growing_from_an_empty_baseline_is_compatible() {
    let (_d, b, c) = tree(EMPTY_CONTRACT, BASELINE);
    run_schema_check(&b, &c, SchemaCheckFormat::Text, false, None)
        .expect("publishing a new workflow must not fail the gate");
}

// ── The baseline must be CURRENT, not merely non-breaking ───────────────────

/// A compatible delta left unabsorbed makes the artifact a record of what was
/// deployed *some time ago*, and the gate reads it as the record of what was
/// deployed *last*. That gap is round-trippable:
///
/// 1. add an enum variant — compatible, so the gate passes and nobody
///    regenerates the artifact;
/// 2. deploy, and record payloads carrying the new variant;
/// 3. remove the variant again — the generated contract now equals the stale
///    baseline, so BOTH checks see zero deltas.
///
/// Replay of anything written by the intermediate release fails, and no step
/// ever reported a delta. `--require-current` closes it by refusing any
/// unabsorbed delta, compatible ones included.
#[test]
fn an_unabsorbed_compatible_delta_fails_the_currency_check() {
    let (_d, b, c) = tree(BASELINE, CURRENT_COMPATIBLE);

    // The ordinary check passes — the change really is safe to deploy.
    assert!(
        run_schema_check(&b, &c, SchemaCheckFormat::Text, false, None).is_ok(),
        "a compatible change is not breaking, and must not be reported as such"
    );

    let err = run_schema_check(&b, &c, SchemaCheckFormat::Text, true, None)
        .expect_err("a stale baseline must fail the currency check");
    assert_eq!(err.exit_code(), 1, "the gate exits 1");
    let msg = format!("{err}");
    assert!(
        msg.contains("schema update"),
        "the failure must name the one command that fixes it: {msg}"
    );
}

/// The A→B→A round trip the currency check exists to prevent, end to end: with
/// the baseline advanced at step 1, removing the variant is caught as breaking.
#[test]
fn advancing_the_baseline_makes_the_later_removal_visible() {
    let dir = tempfile::tempdir().unwrap();
    let b = dir.path().join("baseline.json");
    let c = dir.path().join("current.json");

    // Step 1: absorb the compatible addition, as the currency check forces.
    std::fs::write(&b, BASELINE).unwrap();
    std::fs::write(&c, CURRENT_COMPATIBLE).unwrap();
    run_schema_update(&b, &c, None, None).expect("a compatible update needs no acknowledgement");
    assert!(
        run_schema_check(&b, &c, SchemaCheckFormat::Text, true, None).is_ok(),
        "the absorbed baseline is current"
    );

    // Step 3: the field goes away again. Against the ADVANCED baseline this is
    // a removal — which is exactly what replay of step-2 data would hit.
    std::fs::write(&c, BASELINE).unwrap();
    let err = run_schema_check(&b, &c, SchemaCheckFormat::Text, false, None)
        .expect_err("removing a field recorded by the intermediate release is breaking");
    assert_eq!(err.exit_code(), 1, "the gate exits 1");
}

/// The over-firing guard: an artifact that IS current passes, so the check
/// cannot simply reject everything.
#[test]
fn a_current_baseline_passes_the_currency_check() {
    let (_d, b, c) = tree(BASELINE, BASELINE);
    assert!(
        run_schema_check(&b, &c, SchemaCheckFormat::Text, true, None).is_ok(),
        "an artifact equal to the generated contract is current"
    );
}

/// Currency is about deltas, not about the acknowledgement log: an artifact
/// carrying records still passes once its schemas match.
#[test]
fn an_acknowledged_baseline_is_current_once_absorbed() {
    let dir = tempfile::tempdir().unwrap();
    let b = dir.path().join("baseline.json");
    let c = dir.path().join("current.json");
    std::fs::write(&b, acknowledged_head()).unwrap();
    std::fs::write(&c, CURRENT_BREAKING).unwrap();
    assert!(
        run_schema_check(&b, &c, SchemaCheckFormat::Text, true, None).is_ok(),
        "an absorbed, acknowledged artifact is current"
    );
}

// ── AC-5: the escape hatch must not be bypassable by hand-editing ────────────

/// The artifact after a legitimate `schema update --acknowledge`: it matches
/// `CURRENT_BREAKING` and records why the break was accepted.
fn acknowledged_head() -> String {
    let base = autumn_harvest::WorkflowSchemaContract::parse(BASELINE).unwrap();
    let cur = autumn_harvest::WorkflowSchemaContract::parse(CURRENT_BREAKING).unwrap();
    let head = base
        .acknowledged_update(&cur, "renamed for GDPR", Some("#794"))
        .expect("acknowledging must succeed");
    serde_json::to_string_pretty(&head).unwrap()
}

/// A baseline hand-edited to match the generated contract, with no record.
/// This is the bypass: the ordinary check is clean, so only the comparison
/// against the PREVIOUS revision of the artifact can catch it.
#[test]
fn a_hand_edited_baseline_fails_the_escape_hatch_check() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("base.json"); // previous revision of the artifact
    let generated = dir.path().join("generated.json");
    let head = dir.path().join("head.json"); // artifact as it stands now
    std::fs::write(&base, BASELINE).unwrap();
    std::fs::write(&generated, CURRENT_BREAKING).unwrap();
    std::fs::write(&head, CURRENT_BREAKING).unwrap(); // hand-copied, no ack

    // The ordinary in-PR check passes — that is precisely the problem.
    assert!(
        run_schema_check(&head, &generated, SchemaCheckFormat::Text, false, None).is_ok(),
        "the in-PR check is clean by construction; the bypass is invisible to it"
    );

    let err = run_schema_check(
        &base,
        &generated,
        SchemaCheckFormat::Text,
        false,
        Some(&head),
    )
    .expect_err("an unrecorded break must fail the escape-hatch check");
    assert_eq!(err.exit_code(), 1, "the gate exits 1");
    let msg = format!("{err}");
    assert!(
        msg.contains("acknowledgement"),
        "the failure must name the missing acknowledgement: {msg}"
    );
}

/// The legitimate path passes: `schema update --acknowledge` records the break.
#[test]
fn an_acknowledged_baseline_passes_the_escape_hatch_check() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("base.json");
    let generated = dir.path().join("generated.json");
    let head = dir.path().join("head.json");
    std::fs::write(&base, BASELINE).unwrap();
    std::fs::write(&generated, CURRENT_BREAKING).unwrap();
    std::fs::write(&head, acknowledged_head()).unwrap();

    let out = run_schema_check(
        &base,
        &generated,
        SchemaCheckFormat::Text,
        false,
        Some(&head),
    );
    assert!(
        out.is_ok(),
        "a recorded, justified break must pass the escape-hatch check: {out:?}"
    );
}

/// A rubber stamp must not buy coverage — and must say so.
///
/// `schema update --acknowledge ""` is refused at the authoring path, and the
/// ordinary check reports a blank reason in the artifact it diffs. Neither
/// covers THIS mode run on its own: the head artifact is loaded separately from
/// the two contracts being diffed, so nothing had inspected its reasons before
/// counting them as coverage.
#[test]
fn a_blank_reason_record_fails_the_escape_hatch_check() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("base.json");
    let generated = dir.path().join("generated.json");
    let head = dir.path().join("head.json");
    std::fs::write(&base, BASELINE).unwrap();
    std::fs::write(&generated, CURRENT_BREAKING).unwrap();

    // Take the legitimately-acknowledged artifact and blank every reason,
    // leaving the covering identities intact — the exact hand-edit that would
    // otherwise absorb the break.
    let mut artifact = autumn_harvest::WorkflowSchemaContract::parse(&acknowledged_head()).unwrap();
    assert!(
        !artifact.acknowledged_breaking_changes.is_empty(),
        "fixture must carry records to blank out"
    );
    for a in &mut artifact.acknowledged_breaking_changes {
        a.reason = "   ".to_string();
    }
    std::fs::write(&head, serde_json::to_string_pretty(&artifact).unwrap()).unwrap();

    let err = run_schema_check(
        &base,
        &generated,
        SchemaCheckFormat::Text,
        false,
        Some(&head),
    )
    .expect_err("a blank justification must not cover a break");
    assert_eq!(err.exit_code(), 1, "the gate exits 1");
    let msg = format!("{err}");
    assert!(
        msg.contains("no justification") || msg.contains("rubber stamp"),
        "the failure must name the blank reason as the root cause: {msg}"
    );
}

/// A compatible change needs no record and must not be blocked.
#[test]
fn a_compatible_change_passes_the_escape_hatch_check() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("base.json");
    let generated = dir.path().join("generated.json");
    let head = dir.path().join("head.json");
    std::fs::write(&base, BASELINE).unwrap();
    std::fs::write(&generated, CURRENT_COMPATIBLE).unwrap();
    std::fs::write(&head, CURRENT_COMPATIBLE).unwrap();

    assert!(
        run_schema_check(
            &base,
            &generated,
            SchemaCheckFormat::Text,
            false,
            Some(&head)
        )
        .is_ok(),
        "a compatible change must not need an acknowledgement"
    );
}

/// Retargeting an existing record rather than appending is caught.
///
/// The coverage check alone cannot see it: the retargeted record names this
/// PR's break and is absent from the base multiset, so it looks like fresh
/// coverage. Its one signature is that the record the base carried is gone.
#[test]
fn a_retargeted_acknowledgement_record_fails_the_escape_hatch_check() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("base.json");
    let generated = dir.path().join("generated.json");
    let head = dir.path().join("head.json");

    // The base revision already carries a record for an earlier, unrelated break.
    let mut base_artifact = autumn_harvest::WorkflowSchemaContract::parse(BASELINE).unwrap();
    let stale = autumn_harvest::AcknowledgedBreakingChange {
        workflow: "some_other_workflow".to_string(),
        role: Some(autumn_harvest::SchemaRole::Output),
        field_path: "/legacy".to_string(),
        change: autumn_harvest::ChangeKind::PropertyRemoved,
        reason: "an EARLIER, unrelated break".to_string(),
        recorded_in: Some("#111".to_string()),
    };
    base_artifact.acknowledged_breaking_changes = vec![stale];
    std::fs::write(&base, serde_json::to_string_pretty(&base_artifact).unwrap()).unwrap();
    std::fs::write(&generated, CURRENT_BREAKING).unwrap();

    // Head EDITS that record to name this PR's break instead of appending.
    let mut head_artifact =
        autumn_harvest::WorkflowSchemaContract::parse(CURRENT_BREAKING).unwrap();
    let diff = autumn_harvest::diff_schema_contracts(&base_artifact, &head_artifact);
    head_artifact.acknowledged_breaking_changes = diff
        .breaking()
        .map(|d| autumn_harvest::AcknowledgedBreakingChange {
            workflow: d.workflow.clone(),
            role: d.role,
            field_path: d.field_path.clone(),
            change: d.change,
            reason: "retargeted".to_string(),
            recorded_in: Some("#111".to_string()),
        })
        .collect();
    std::fs::write(&head, serde_json::to_string_pretty(&head_artifact).unwrap()).unwrap();

    let err = run_schema_check(
        &base,
        &generated,
        SchemaCheckFormat::Text,
        false,
        Some(&head),
    )
    .expect_err("a rewritten audit log must fail the escape-hatch check");
    assert_eq!(err.exit_code(), 1, "the gate exits 1");
    assert!(
        matches!(
            err,
            autumn_harvest_cli::CliError::SchemaContractAuditLogRewritten { .. }
        ),
        "the remedy differs from 'record it', so the error must too: {err:?}"
    );
    let msg = format!("{err}");
    assert!(
        msg.contains("some_other_workflow") && msg.contains("append-only"),
        "the failure must name the vanished record and the rule: {msg}"
    );
}

/// A legitimate acknowledged update APPENDS, so the audit-log check is silent
/// even when the base already carried unrelated records.
#[test]
fn appending_to_a_non_empty_acknowledgement_log_passes() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("base.json");
    let generated = dir.path().join("generated.json");
    let head = dir.path().join("head.json");

    let mut base_artifact = autumn_harvest::WorkflowSchemaContract::parse(BASELINE).unwrap();
    base_artifact
        .acknowledged_breaking_changes
        .push(autumn_harvest::AcknowledgedBreakingChange {
            workflow: "some_other_workflow".to_string(),
            role: Some(autumn_harvest::SchemaRole::Output),
            field_path: "/legacy".to_string(),
            change: autumn_harvest::ChangeKind::PropertyRemoved,
            reason: "an EARLIER, unrelated break".to_string(),
            recorded_in: Some("#111".to_string()),
        });
    let cur = autumn_harvest::WorkflowSchemaContract::parse(CURRENT_BREAKING).unwrap();
    let head_artifact = base_artifact
        .acknowledged_update(&cur, "renamed for GDPR", Some("#794"))
        .expect("acknowledging must succeed");

    std::fs::write(&base, serde_json::to_string_pretty(&base_artifact).unwrap()).unwrap();
    std::fs::write(&generated, CURRENT_BREAKING).unwrap();
    std::fs::write(&head, serde_json::to_string_pretty(&head_artifact).unwrap()).unwrap();

    let out = run_schema_check(
        &base,
        &generated,
        SchemaCheckFormat::Text,
        false,
        Some(&head),
    );
    assert!(out.is_ok(), "an append-only update must pass: {out:?}");
}

/// Omitting the flag leaves the ordinary check byte-for-byte unchanged.
#[test]
fn the_escape_hatch_check_is_opt_in() {
    let (_d, b, c) = tree(BASELINE, CURRENT_BREAKING);
    let err = run_schema_check(&b, &c, SchemaCheckFormat::Text, false, None)
        .expect_err("the ordinary check still fails on a breaking delta");
    assert!(
        matches!(
            err,
            autumn_harvest_cli::CliError::SchemaContractBreaking { .. }
        ),
        "without the flag the ordinary breaking-change error is reported: {err:?}"
    );
}

/// The binary must survive a **1 MiB main-thread stack**, which is what Windows
/// gives every process by default.
///
/// A debug build of a CLI this size overflows that inside clap's own
/// argument-tree construction — before any subcommand is dispatched, so even
/// `--help` aborts with `STATUS_STACK_OVERFLOW`. Linux's 8 MiB default hides it,
/// which is why it stayed latent until this suite became the first thing in the
/// repo to spawn `harvest` as a process.
///
/// `ulimit -s` bounds the **main** thread only; a thread's stack is sized at
/// spawn, so this passes exactly when `main` does its work on one it sized
/// itself. Unix-only because that is where `ulimit` exists — Windows CI covers
/// the same guarantee natively by running the three tests above.
#[cfg(unix)]
#[test]
fn the_real_binary_survives_a_one_mebibyte_main_thread_stack() {
    for args in [
        vec!["--help"],
        vec!["schema", "check", "--help"],
        vec!["det-check", "--help"],
    ] {
        let out = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg(format!(
                "ulimit -s 1024; exec {} {}",
                env!("CARGO_BIN_EXE_harvest"),
                args.join(" ")
            ))
            .output()
            .expect("the harvest binary must run under /bin/sh");

        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            !stderr.contains("overflowed its stack") && !stderr.contains("stack overflow"),
            "`harvest {}` overflowed a 1 MiB main-thread stack — this is what \
             Windows aborts on:\n{stderr}",
            args.join(" ")
        );
        assert_eq!(
            out.status.code(),
            Some(0),
            "`harvest {}` must exit 0 under a 1 MiB stack; stderr:\n{stderr}",
            args.join(" ")
        );
    }
}
