//! `cargo xtask check underscore-input-snippets`: the port of
//! `scripts/check-underscore-input-snippets.sh`.
//!
//! The guard fails if the `last_completion_result` snippet of chapter 8 or
//! the `WorkflowTestEnv` billing-loop snippet of chapter 11 stop compiling
//! against the real crate.
//!
//! Mechanism this guards against: both snippets declare their workflow input
//! as `_: ()`. `#[workflow]` builds its dispatch code from the identifiers of
//! the input parameters, and `attr_util::param_idents` silently drops a `_`
//! pattern. The generated call then passes one argument to a two-argument
//! function. rustc reports `error[E0061]: this function takes 5 arguments but
//! 4 arguments were supplied ... argument #5 of type TypedStartOptions is
//! missing` on the `#[workflow]` line. The message names neither `_` nor the
//! fix.
//!
//! The chapter 11 snippet also passed a `Duration` to `ctx.timer`, which takes
//! whole seconds (`u64`).

use std::path::Path;
use std::process::ExitCode;

use smop::prelude::*;

use super::support::{self, Report, ScratchExample, SnippetCompile};
use crate::util;

const EXAMPLE_NAME: &str = "_onramp_doc_check_underscore_input";
const CH8: &str = "docs/getting-started/08-dags-and-schedules.md";
const CH11: &str = "docs/getting-started/11-testing.md";

/// Arguments of `cargo xtask check underscore-input-snippets`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// Runs `cargo xtask check underscore-input-snippets`.
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
            ok_line: "OK: chapter 8 and chapter 11 snippets compile.",
            failure_head: "chapter 8/11 snippets do not compile",
            count_noun: "error(s)",
            error_prefix: "error[",
        }
        .run(&root, &example, &source)?,
        Err(report) => report,
    };
    Ok(report.emit())
}

/// Returns the first of the first 30 blocks of chapter 8 that defines
/// `incremental_etl`.
///
/// The guard finds the block by its content, so an inserted earlier block
/// cannot shift it. An empty block ends the search, as in the old guard.
fn incremental_etl_block(doc: &str) -> String {
    for n in 1..=30 {
        let block = support::rust_block(doc, n);
        if block.is_empty() {
            break;
        }
        if block.contains("async fn incremental_etl") {
            return block;
        }
    }
    String::new()
}

/// Builds the example source from chapters 8 and 11 under `root`.
///
/// An `Err` holds the report of a chapter that lacks its snippet.
fn render(root: &Path) -> std::result::Result<String, Report> {
    let b8 = incremental_etl_block(&support::read_or_empty(&root.join(CH8)));
    let b11 = support::rust_block(&support::read_or_empty(&root.join(CH11)), 2);
    if b8.is_empty() || !b11.contains("async fn billing_cycle") {
        let mut report = Report::new();
        report.fail(
            "could not extract the ch8 incremental_etl block or the ch11 billing_cycle block; has a chapter been restructured? Update this guard.",
        );
        return Err(report);
    }
    Ok(support::echo_lines(&[
        "#![allow(dead_code, unused)]",
        "mod ch8 {",
        &b8,
        "}",
        "mod ch11 {",
        "use autumn_harvest::testing::WorkflowTestEnv;",
        &b11,
        "}",
        "fn main() {}",
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(ch8: &str, ch11: &str) -> smop::fs::TempDir {
        let dir = fs::temp_dir().expect("temp dir");
        std::fs::create_dir_all(dir.path().join("docs/getting-started")).expect("mkdir");
        std::fs::write(dir.path().join(CH8), ch8).expect("ch8");
        std::fs::write(dir.path().join(CH11), ch11).expect("ch11");
        dir
    }

    const CH11_OK: &str = "```rust\nfirst\n```\n```rust\nasync fn billing_cycle() {}\n```\n";

    #[test]
    fn render_finds_the_ch8_block_by_content() {
        let dir = fixture(
            "```rust\nother\n```\n```rust\nasync fn incremental_etl() {}\n```\n",
            CH11_OK,
        );
        let source = render(dir.path()).expect("render");
        assert_eq!(
            source,
            "#![allow(dead_code, unused)]\nmod ch8 {\nasync fn incremental_etl() {}\n}\nmod ch11 {\nuse autumn_harvest::testing::WorkflowTestEnv;\nasync fn billing_cycle() {}\n}\nfn main() {}\n"
        );
    }

    #[test]
    fn an_empty_block_ends_the_search() {
        let doc = "```rust\n```\n```rust\nasync fn incremental_etl() {}\n```\n";
        assert_eq!(incremental_etl_block(doc), "");
    }

    #[test]
    fn a_ch11_block_without_billing_cycle_fails() {
        let dir = fixture(
            "```rust\nasync fn incremental_etl() {}\n```\n",
            "```rust\na\n```\n```rust\nb\n```\n",
        );
        let report = render(dir.path()).expect_err("missing");
        assert_eq!(report.code(), 1);
        assert!(
            report
                .stderr()
                .starts_with("could not extract the ch8 incremental_etl block")
        );
    }
}
