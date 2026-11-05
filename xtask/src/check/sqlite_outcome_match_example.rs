//! `cargo xtask check sqlite-outcome-match-example`: the port of
//! `scripts/check-sqlite-outcome-match-example.sh`.
//!
//! The guard fails if the §7 "drive model" example of
//! `docs/sqlite-backend.md` regresses into a real compile failure. The
//! defect is a `match rt.outcome(exec)? { ... }` that does not cover every
//! `ExecutionOutcome` variant.
//!
//! Mechanism: one commit (3abb5b6, issue #1348) introduced both the doc and
//! `ExecutionOutcome::Terminated`. Its author added the variant to the enum
//! but never added the matching arm to the `match` of the guide. So the
//! snippet was broken from the day that it was written.
//!
//! Two existing checks cannot catch a non-exhaustive `match`. The corpus of
//! `cargo xtask audit doc-snippet-syntax` is `docs/getting-started/` and
//! `README.md` only, and this file is outside it. `rustfmt` is a syntax
//! check, not a compiler. E0004 is a property of the type checker, not a
//! parse error. `examples/quickstart.rs` of the crate already has the correct
//! four-arm match. That is why `cargo test -p autumn-harvest-sqlite` in CI
//! never surfaced this: nothing ever compiled the copy in the doc.
//!
//! This guard compiles the pinned block with `cargo check`. It does not count
//! match arms in the text. A variant added to or removed from
//! `ExecutionOutcome` is exactly the case that a text-shape heuristic misses.
//! A real compile catches it for free.

use std::path::Path;
use std::process::ExitCode;

use smop::prelude::*;

use super::default_run::support::{self, Report, ScratchExample};
use crate::util;

const DOC: &str = "docs/sqlite-backend.md";
const EXAMPLE_NAME: &str = "_onramp_doc_check_sqlite_outcome_match";
const IMPORT: &str = "use autumn_harvest_sqlite::{RunState, ExecutionOutcome};";

/// The stand-in that precedes the pinned block.
///
/// The prose of the doc establishes `rt` (a running `SqliteRuntime`) and
/// `exec` (an in-flight `ExecutionId`) in earlier sections. This stand-in
/// registers a trivial workflow and starts it to supply both. The sibling
/// guards supply context from the chapter in the same way.
const PRELUDE: &str = r#"#![allow(clippy::all, clippy::pedantic, clippy::nursery, dead_code)]
use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::SqliteRuntime;

#[workflow]
async fn onramp_check_workflow(_ctx: &WorkflowContext, _input: ()) -> Result<String, String> {
    Ok(String::new())
}

#[tokio::main]
async fn main() -> Result<(), autumn_harvest_sqlite::SqliteError> {
    let mut rt = SqliteRuntime::open_in_memory()?;
    rt.register_workflow(&onramp_check_workflow_info());
    let exec = rt.start_workflow("onramp_check_workflow", serde_json::json!(null))?;

"#;

/// Arguments of `cargo xtask check sqlite-outcome-match-example`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// Runs `cargo xtask check sqlite-outcome-match-example`.
pub fn run(_args: Args) -> Result<ExitCode> {
    let root = util::repo_root();
    let example = ScratchExample::claim(
        root.join(format!("autumn-harvest-sqlite/examples/{EXAMPLE_NAME}.rs")),
    );
    let report = evaluate(&root, |source| {
        example.write(source)?;
        support::cargo_merged(
            &root,
            &[
                "check",
                "--quiet",
                "-p",
                "autumn-harvest-sqlite",
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
    source.push_str(&support::echo_lines(&[code, "", "    Ok(())", "}"]));
    source
}

/// Checks the doc under `root`. `compile` writes and checks one source.
fn evaluate(root: &Path, compile: impl FnOnce(&str) -> Result<(bool, String)>) -> Result<Report> {
    let mut report = Report::new();
    let path = root.join(DOC);
    if !path.is_file() {
        report.fail(format!("{DOC}: not found"));
        return Ok(report);
    }

    // The window runs from the `RunState`/`ExecutionOutcome` import to the
    // close of the fence. It survives either match growing or shrinking,
    // because it stops at the next fence line and not after a fixed count.
    let code = support::window_from(&support::read_or_empty(&path), IMPORT);
    if code.is_empty() {
        report.fail(format!(
            "{DOC}: could not find the §7 drive-model example (looked for its \"{IMPORT}\" import line); has the chapter been restructured? Update this guard to match."
        ));
        return Ok(report);
    }

    let (passed, output) = compile(&render(&code))?;
    if !passed {
        report.err(format!(
            "{DOC}'s §7 drive-model example does not compile against the real autumn-harvest-sqlite crate."
        ));
        report.err("");
        report.fail(output);
        return Ok(report);
    }

    report.out(format!(
        "OK: {DOC}'s §7 drive-model example compiles against the real autumn-harvest-sqlite crate."
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
    fn render_closes_main_after_the_block() {
        let dir = fixture(&format!(
            "```rust\n{IMPORT}\nmatch rt.outcome(exec)? {{}}\n```\n"
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
            "json!(null))?;\n\n{IMPORT}\nmatch rt.outcome(exec)? {{}}\n\n    Ok(())\n}}\n"
        )));
    }

    #[test]
    fn a_compile_failure_prints_the_output() {
        let dir = fixture(&format!("{IMPORT}\n```\n"));
        let report =
            evaluate(dir.path(), |_| Ok((false, "error[E0004]: x".into()))).expect("evaluate");
        assert_eq!(report.code(), 1);
        assert!(report.stderr().ends_with("crate.\n\nerror[E0004]: x\n"));
    }

    #[test]
    fn a_missing_import_line_fails() {
        let dir = fixture("use autumn_harvest_sqlite::RunState;\n");
        let report = evaluate(dir.path(), |_| panic!("no compile")).expect("evaluate");
        assert!(
            report
                .stderr()
                .contains("could not find the §7 drive-model example")
        );
    }
}
