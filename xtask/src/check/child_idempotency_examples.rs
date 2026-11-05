//! `cargo xtask check child-idempotency-examples`: the port of
//! `scripts/check-child-idempotency-examples.sh`.
//!
//! The guard fails if the flagship snippets of chapter 5 (child workflows) or
//! chapter 6 (idempotency) stop compiling against the real crate.
//!
//! Mechanism this guards against: both chapters show code that a reader
//! pastes verbatim, and both had drifted from the API.
//!
//! - Chapter 5 called `spawn_child_workflow_raw(name, id, input)`. The real
//!   method takes `(name, input)`. The result was `error[E0061]`, "takes 2
//!   arguments but 3 arguments were supplied".
//! - The `charge_card` of chapter 6 returned `HarvestResult<Value>` but ended
//!   a chain with `.map_err(|e| e.to_string())?`. `HarvestError` has no
//!   `From<String>`. The result was `error[E0277]`: the `?` operator cannot
//!   convert the error.
//!
//! The guard extracts the first ```` ```rust ```` block of each chapter. It
//! wraps each block with the imports and stubs that the chapter states or
//! implies, and checks the result with `cargo check`.

use std::path::Path;
use std::process::ExitCode;

use smop::prelude::*;

use super::default_run::support::{self, Report, ScratchExample, SnippetCompile};
use crate::util;

const EXAMPLE_NAME: &str = "_onramp_doc_check_child_idempotency";
const CH5: &str = "docs/getting-started/05-child-workflows.md";
const CH6: &str = "docs/getting-started/06-idempotency.md";

/// Arguments of `cargo xtask check child-idempotency-examples`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// Runs `cargo xtask check child-idempotency-examples`.
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
            ok_line: "OK: chapter 5 and chapter 6 flagship snippets compile.",
            failure_head: "chapter 5/6 snippets do not compile",
            count_noun: "error(s)",
            error_prefix: "error[",
        }
        .run(&root, &example, &source)?,
        Err(report) => report,
    };
    Ok(report.emit())
}

/// Builds the example source from the two chapters under `root`.
///
/// An `Err` holds the report of a chapter that has no block to extract.
fn render(root: &Path) -> std::result::Result<String, Report> {
    let b5 = support::rust_block(&support::read_or_empty(&root.join(CH5)), 1);
    let b6 = support::rust_block(&support::read_or_empty(&root.join(CH6)), 1);
    if b5.is_empty() || b6.is_empty() {
        let mut report = Report::new();
        report.fail(format!(
            "could not extract the first rust block of {CH5} or {CH6}; has a chapter been restructured? Update this guard."
        ));
        return Err(report);
    }
    Ok(support::echo_lines(&[
        "#![allow(dead_code, unused)]",
        "use std::time::Duration;",
        "use autumn_harvest::prelude::*;",
        "mod ch5 { use super::*;",
        &b5,
        "}",
        "mod ch6 { use super::*;",
        "async fn stripe_charge(_a: u64, _c: &str, _k: &str) -> Result<String, String> { Ok(String::new()) }",
        &b6,
        "}",
        "fn main() {}",
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(ch5: &str, ch6: &str) -> smop::fs::TempDir {
        let dir = fs::temp_dir().expect("temp dir");
        let docs = dir.path().join("docs/getting-started");
        std::fs::create_dir_all(&docs).expect("mkdir");
        std::fs::write(dir.path().join(CH5), ch5).expect("ch5");
        std::fs::write(dir.path().join(CH6), ch6).expect("ch6");
        dir
    }

    #[test]
    fn render_wraps_each_first_block_in_its_module() {
        let dir = fixture(
            "```rust\nfn five() {}\n```\n```rust\nfn later() {}\n```\n",
            "```rust\nfn six() {}\n```\n",
        );
        let source = render(dir.path()).expect("render");
        assert!(source.starts_with("#![allow(dead_code, unused)]\nuse std::time::Duration;\n"));
        assert!(source.contains("mod ch5 { use super::*;\nfn five() {}\n}\n"));
        assert!(source.contains("Ok(String::new()) }\nfn six() {}\n}\nfn main() {}\n"));
        assert!(!source.contains("later"));
    }

    #[test]
    fn a_chapter_without_a_block_fails() {
        let dir = fixture("no code\n", "```rust\nfn six() {}\n```\n");
        let report = render(dir.path()).expect_err("no block");
        assert_eq!(report.code(), 1);
        assert!(
            report
                .stderr()
                .starts_with("could not extract the first rust block of")
        );
    }
}
