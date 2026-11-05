//! `cargo xtask check readme-front-door-snippets`: the port of
//! `scripts/check-readme-front-door-snippets.sh`.
//!
//! The guard fails if the cron-schedule snippet or the `ActivityFailure`
//! retry snippet of the README stop compiling against the real crates.
//!
//! Mechanism this guards against: a reader pastes both snippets from the
//! README, the front door. The schedule snippet called `.workflows(..)` and
//! `.workflow_schedule(..)` on `autumn_web::app()`. `AppBuilder` has neither
//! method. `HarvestPlugin` has no `workflow_schedule`, and `HarvestBuilder`
//! is not the app builder. rustc reports
//! `E0599: no method named workflows found for struct AppBuilder`. Nothing in
//! the message points at `HarvestBuilder`, or at the CLI and HTTP schedule
//! routes that the README documents two paragraphs later. The retry snippet
//! names `Duration` but imports only `autumn_harvest::prelude::*`, which
//! does not re-export it (`E0433`).

use std::path::Path;
use std::process::ExitCode;

use smop::prelude::*;

use super::support::{self, Report, ScratchExample, SnippetCompile};
use crate::util;

const EXAMPLE_NAME: &str = "_onramp_doc_check_readme_front_door";

/// Arguments of `cargo xtask check readme-front-door-snippets`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// Runs `cargo xtask check readme-front-door-snippets`.
pub fn run(_args: Args) -> Result<ExitCode> {
    let root = util::repo_root();
    let example = ScratchExample::claim(
        root.join(format!("autumn-harvest-plugin/examples/{EXAMPLE_NAME}.rs")),
    );
    let report = match render(&root) {
        Ok(source) => SnippetCompile {
            cargo_args: &[
                "check",
                "--quiet",
                "-p",
                "autumn-harvest-plugin",
                "--example",
                EXAMPLE_NAME,
            ],
            ok_line: "OK: README schedule and retry snippets compile.",
            failure_head: "README front-door snippets do not compile",
            count_noun: "error(s)",
            error_prefix: "error[",
        }
        .run(&root, &example, &source)?,
        Err(report) => report,
    };
    Ok(report.emit())
}

/// Returns the first ```` ```rust ```` fence body of `readme` that holds
/// `needle`.
///
/// This copies the `readme_block` awk function of the old guard. A
/// ```` ```rust ```` line always starts a fresh buffer, even inside an open
/// fence.
fn readme_block(readme: &str, needle: &str) -> String {
    let mut inside = false;
    let mut buffer = String::new();
    for line in support::lines(readme) {
        if line == "```rust" {
            buffer.clear();
            inside = true;
            continue;
        }
        if inside && line == "```" {
            inside = false;
            if buffer.contains(needle) {
                return support::trim_newlines(&buffer).to_owned();
            }
            continue;
        }
        if inside {
            buffer.push_str(line);
            buffer.push('\n');
        }
    }
    String::new()
}

/// Builds the example source from `README.md` under `root`.
///
/// An `Err` holds the report of a README that lacks one of the snippets.
fn render(root: &Path) -> std::result::Result<String, Report> {
    let readme = support::read_or_empty(&root.join("README.md"));
    let schedule = readme_block(&readme, "WorkflowSchedule::new(");
    let retry = readme_block(&readme, "ActivityFailure::non_retryable");
    if schedule.is_empty() || retry.is_empty() {
        let mut report = Report::new();
        report.fail(
            "could not extract the README WorkflowSchedule block or the ActivityFailure block; has the README been restructured? Update this guard.",
        );
        return Err(report);
    }
    // The stub workflow lives in its own module, so the snippet must carry
    // its own `use` lines. The glob re-exports only the workflow items.
    Ok(support::echo_lines(&[
        "#![allow(dead_code, unused)]",
        "mod stub {",
        "use autumn_harvest::prelude::*;",
        "#[workflow]",
        "pub async fn daily_billing_report(ctx: &WorkflowContext, input: serde_json::Value) -> HarvestResult<()> { Ok(()) }",
        "}",
        "mod schedule {",
        "use super::stub::*;",
        "fn wiring() -> Result<(), Box<dyn std::error::Error>> {",
        &schedule,
        "Ok(()) }",
        "}",
        "mod retry {",
        &retry,
        "}",
        "fn main() {}",
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(readme: &str) -> smop::fs::TempDir {
        let dir = fs::temp_dir().expect("temp dir");
        std::fs::write(dir.path().join("README.md"), readme).expect("readme");
        dir
    }

    const README: &str = "```rust\nlet a = 1;\n```\n```rust\nlet s = WorkflowSchedule::new(\"daily\");\n```\n```rust\nuse std::time::Duration;\nErr(ActivityFailure::non_retryable(\"x\"))\n```\n";

    #[test]
    fn render_places_each_snippet_in_its_module() {
        let dir = fixture(README);
        let source = render(dir.path()).expect("render");
        assert!(source.contains(
            "Box<dyn std::error::Error>> {\nlet s = WorkflowSchedule::new(\"daily\");\nOk(()) }\n"
        ));
        assert!(source.contains("mod retry {\nuse std::time::Duration;\nErr(ActivityFailure::non_retryable(\"x\"))\n}\nfn main() {}\n"));
        assert!(!source.contains("let a = 1;"));
    }

    #[test]
    fn an_unclosed_fence_never_matches() {
        assert_eq!(
            readme_block(
                "```rust\nWorkflowSchedule::new(\n",
                "WorkflowSchedule::new("
            ),
            ""
        );
    }

    #[test]
    fn a_missing_snippet_fails() {
        let dir = fixture("```rust\nWorkflowSchedule::new(\n```\n");
        let report = render(dir.path()).expect_err("missing retry");
        assert_eq!(report.code(), 1);
        assert!(
            report
                .stderr()
                .starts_with("could not extract the README WorkflowSchedule block")
        );
    }
}
