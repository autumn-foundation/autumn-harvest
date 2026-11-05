//! `cargo xtask check durable-timers-examples`: the port of
//! `scripts/check-durable-timers-examples.sh`.
//!
//! The guard fails if the flagship snippets of chapter 3 (durable timers)
//! stop compiling against the real crate.
//!
//! Mechanism this guards against: the chapter shows code that a reader pastes
//! verbatim. Nothing compiled it, so drift in the timer API went unseen.
//!
//! The guard extracts the first three ```` ```rust ```` blocks: the sleep
//! timer, the cancellable timer and the business-day timer. It wraps them
//! with the imports and type stubs that the chapter implies, and checks the
//! result with `cargo check`.

use std::path::Path;
use std::process::ExitCode;

use smop::prelude::*;

use super::support::{self, Report, ScratchExample, SnippetCompile};
use crate::util;

const EXAMPLE_NAME: &str = "_onramp_doc_check_durable_timers";
const CH3: &str = "docs/getting-started/03-durable-timers.md";

/// Arguments of `cargo xtask check durable-timers-examples`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// Runs `cargo xtask check durable-timers-examples`.
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
            ok_line: "OK: chapter 3 flagship snippets compile.",
            failure_head: "chapter 3 snippets do not compile",
            count_noun: "error(s)",
            error_prefix: "error[",
        }
        .run(&root, &example, &source)?,
        Err(report) => report,
    };
    Ok(report.emit())
}

/// Builds the example source from chapter 3 under `root`.
///
/// An `Err` holds the report of a chapter with fewer than three blocks.
fn render(root: &Path) -> std::result::Result<String, Report> {
    let doc = support::read_or_empty(&root.join(CH3));
    let b1 = support::rust_block(&doc, 1);
    let b2 = support::rust_block(&doc, 2);
    let b3 = support::rust_block(&doc, 3);
    if b1.is_empty() || b2.is_empty() || b3.is_empty() {
        let mut report = Report::new();
        report.fail(format!(
            "could not extract the first three rust blocks of {CH3}; has the chapter been restructured? Update this guard."
        ));
        return Err(report);
    }
    Ok(support::echo_lines(&[
        "#![allow(dead_code, unused)]",
        "use autumn_harvest::prelude::*;",
        "use serde::{Deserialize, Serialize};",
        "#[derive(Serialize, Deserialize)] struct Order { items: Vec<String>, shipped_early: bool }",
        "#[derive(Serialize, Deserialize)] struct Ticket { id: i64 }",
        "mod timers { use super::*;",
        &b1,
        &b2,
        &b3,
        "}",
        "fn main() {}",
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(body: &str) -> smop::fs::TempDir {
        let dir = fs::temp_dir().expect("temp dir");
        let doc = dir.path().join(CH3);
        std::fs::create_dir_all(doc.parent().expect("parent")).expect("mkdir");
        std::fs::write(doc, body).expect("doc");
        dir
    }

    #[test]
    fn render_puts_three_blocks_in_one_module() {
        let dir = fixture(
            "```rust\none\n```\n```rust\ntwo\n```\n```rust\nthree\n```\n```rust\nfour\n```\n",
        );
        let source = render(dir.path()).expect("render");
        assert!(source.ends_with("mod timers { use super::*;\none\ntwo\nthree\n}\nfn main() {}\n"));
        assert!(!source.contains("four"));
    }

    #[test]
    fn two_blocks_are_not_enough() {
        let dir = fixture("```rust\none\n```\n```rust\ntwo\n```\n");
        let report = render(dir.path()).expect_err("too few");
        assert_eq!(report.code(), 1);
        assert!(
            report
                .stderr()
                .contains("could not extract the first three rust blocks")
        );
    }
}
