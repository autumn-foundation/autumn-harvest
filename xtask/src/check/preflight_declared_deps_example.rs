//! `cargo xtask check preflight-declared-deps-example`: the port of
//! `scripts/check-preflight-declared-deps-example.sh`.
//!
//! The guard fails if the "Catching a forgotten registration before rollout"
//! example of chapter 10 regresses into one of two real compile failures. The
//! example is the flagship `#[workflow(activities = [...], children = [...])]`
//! snippet. An Onramp clean-room pass found both failures.
//!
//! 1. `ctx.execute_activity(&send_email_info(), user_id).await?` and the
//!    `spawn_child_workflow` call below it are `HarvestResult<_>`, which is
//!    `Result<_, HarvestError>`. But the workflow returns `Result<_, String>`.
//!    There is no `From<HarvestError> for String`. So `?` needs a real
//!    conversion first, or rustc reports E0277. The `continue_as_new`
//!    example of chapter 7 had the same defect class (PR #1700).
//! 2. A `.map_err(|e| e.to_string())` alone does not fix it. The output type
//!    `O` of `execute_activity` is generic. It stays unconstrained when a
//!    bare statement discards the result. The return type of
//!    `continue_as_new` is the concrete `HarvestResult<()>`, so that call has
//!    no such gap.
//!
//! Current stable rustc denies the second failure by default on every
//! edition, not just 2024. The lint is `dependency_on_unit_never_type_fallback`,
//! part of `rust_2024_compatibility`. A live check confirmed it in a
//! standalone edition-2021 crate. That crate depended on the autumn-harvest
//! of this workspace by path, like the pinned `edition = "2021"` tutorial
//! setup of chapter 1.
//!
//! The fix is `execute_activity::<_, serde_json::Value>(...)`. It is NOT the
//! `()` that rustc suggests as a generic fallback (PR #1708).
//! `ActivityInfo` is not generic, so `execute_activity` never checks that
//! `O` matches the return type of the named activity. `charge_card` here is
//! a stand-in. But the real activity of that name in
//! `docs/getting-started/06-idempotency.md` returns
//! `HarvestResult<serde_json::Value>`, a JSON object. A pin of `O = ()`
//! would still compile. It would then fail at runtime with
//! `HarvestError::Serialization` as soon as a reader wires the example to
//! that real activity. That is a worse failure than the compile error that
//! this guard exists to catch. Any output of an activity deserializes into
//! `serde_json::Value`. So it is the one annotation that is safe for the
//! real output shape of any activity.
//!
//! Neither failure is a syntax error. So the rustfmt-only check of
//! `cargo xtask audit doc-snippet-syntax` passes this block clean. That
//! audit only checks that the block parses, and never compiles it. This
//! guard compiles the pinned block with `cargo check`, like its sibling
//! guards, and does not pattern-match stand-in text. Whether a `?`
//! conversion or a generic output type resolves is a property of the type
//! checker, not a text shape.

use std::path::Path;
use std::process::ExitCode;

use smop::prelude::*;

use super::default_run::support::{self, Report, ScratchExample};
use crate::util;

const DOC: &str = "docs/getting-started/10-operations.md";
const EXAMPLE_NAME: &str = "_onramp_doc_check_preflight_declared_deps";
const ATTRIBUTE: &str =
    "#[workflow(activities = [send_email, charge_card], children = [generate_report])]";

/// The stand-ins that precede the pinned block.
///
/// The doc prose and the pinned block do not define `send_email`,
/// `charge_card`, `generate_report` or `Report`. The doc assumes context from
/// earlier chapters: the activities of chapter 2 and the child workflows of
/// chapter 5. Stand-ins with the same names let the pinned block compile
/// unmodified.
///
/// The return type of the `charge_card` stand-in deliberately matches the
/// real activity of that name in `06-idempotency.md`: a JSON object, not
/// unit. A stand-in that narrowed it to `()` would hide exactly the
/// output-type mismatch that this guard exists to catch (PR #1708).
const PRELUDE: &str = r#"#![allow(clippy::all, clippy::pedantic, clippy::nursery, dead_code)]
use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Report {
    ok: bool,
}

#[activity]
async fn send_email(_ctx: &ActivityContext, _user_id: i64) -> Result<serde_json::Value, String> {
    Ok(serde_json::Value::Null)
}

// Mirrors the real charge_card in docs/getting-started/06-idempotency.md:
// HarvestResult<serde_json::Value>, not unit.
#[activity]
async fn charge_card(_ctx: &ActivityContext, _user_id: i64) -> Result<serde_json::Value, String> {
    Ok(serde_json::json!({ "charge_id": "ch_stub" }))
}

#[workflow]
async fn generate_report(_ctx: &WorkflowContext, _user_id: i64) -> Result<Report, String> {
    Ok(Report { ok: true })
}

"#;

/// Arguments of `cargo xtask check preflight-declared-deps-example`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// Runs `cargo xtask check preflight-declared-deps-example`.
pub fn run(_args: Args) -> Result<ExitCode> {
    let root = util::repo_root();
    let example =
        ScratchExample::claim(root.join(format!("autumn-harvest/examples/{EXAMPLE_NAME}.rs")));
    let report = evaluate(&root, |source| {
        example.write(source)?;
        support::cargo_merged(
            &root,
            &[
                "check",
                "--quiet",
                "-p",
                "autumn-harvest",
                "--example",
                EXAMPLE_NAME,
            ],
        )
    })?;
    Ok(report.emit())
}

/// Builds the example source around the pinned block.
fn render(code: &str) -> String {
    let mut source = PRELUDE.to_owned();
    source.push_str(&support::echo_lines(&[code, "", "fn main() {}"]));
    source
}

/// Checks the chapter under `root`. `compile` writes and checks one source.
fn evaluate(root: &Path, compile: impl FnOnce(&str) -> Result<(bool, String)>) -> Result<Report> {
    let mut report = Report::new();
    let path = root.join(DOC);
    if !path.is_file() {
        report.fail(format!("{DOC}: not found"));
        return Ok(report);
    }

    // The window runs from the attribute line to the close of the fence. It
    // survives a body that grows or shrinks, because it stops at the next
    // fence line and not after a fixed line count.
    let code = support::window_from(&support::read_or_empty(&path), ATTRIBUTE);
    if code.is_empty() {
        report.fail(format!(
            "{DOC}: could not find the \"Catching a forgotten registration before rollout\" example (looked for its #[workflow(activities = [...], children = [...])] attribute line); has the chapter been restructured? Update this guard to match."
        ));
        return Ok(report);
    }

    let (passed, output) = compile(&render(&code))?;
    if !passed {
        report.err(format!(
            "{DOC}'s \"Catching a forgotten registration before rollout\" example does not compile against the real autumn-harvest crate."
        ));
        report.err("");
        report.fail(output);
        return Ok(report);
    }

    report.out(format!(
        "OK: {DOC}'s preflight-declared-deps example compiles against the real autumn-harvest crate."
    ));
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(body: &str) -> smop::fs::TempDir {
        let dir = fs::temp_dir().expect("temp dir");
        let doc = dir.path().join(DOC);
        std::fs::create_dir_all(doc.parent().expect("parent")).expect("mkdir");
        std::fs::write(doc, body).expect("doc");
        dir
    }

    #[test]
    fn the_pinned_block_follows_the_stand_ins() {
        let dir = fixture(&format!(
            "```rust\n{ATTRIBUTE}\nasync fn onboard() {{}}\n```\n"
        ));
        let mut seen = String::new();
        let report = evaluate(dir.path(), |source| {
            seen = source.to_owned();
            Ok((true, String::new()))
        })
        .expect("evaluate");
        assert_eq!(report.code(), 0);
        assert!(seen.starts_with(PRELUDE));
        assert!(seen.ends_with(&format!(
            "\n\n{ATTRIBUTE}\nasync fn onboard() {{}}\n\nfn main() {{}}\n"
        )));
    }

    #[test]
    fn a_compile_failure_prints_the_output() {
        let dir = fixture(&format!("{ATTRIBUTE}\nx\n```\n"));
        let report =
            evaluate(dir.path(), |_| Ok((false, "error[E0277]: no".into()))).expect("evaluate");
        assert_eq!(report.code(), 1);
        assert!(report.stderr().ends_with("crate.\n\nerror[E0277]: no\n"));
    }

    #[test]
    fn a_missing_example_or_doc_fails_before_compiling() {
        let dir = fixture("no example\n");
        let report = evaluate(dir.path(), |_| panic!("no compile")).expect("evaluate");
        assert!(
            report
                .stderr()
                .contains("could not find the \"Catching a forgotten registration")
        );
        let empty = fs::temp_dir().expect("temp dir");
        let report = evaluate(empty.path(), |_| panic!("no compile")).expect("evaluate");
        assert_eq!(report.stderr(), format!("{DOC}: not found\n"));
    }
}
