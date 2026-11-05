//! `cargo xtask check worker-routing-examples`: the port of
//! `scripts/check-worker-routing-examples.sh`.
//!
//! The guard fails if the snippets of chapter 9 (worker routing) stop
//! compiling against the real crate.
//!
//! Mechanism this guards against: the chapter shows code that a reader pastes
//! verbatim. Nothing compiled it, so drift in the `requires` grammar or in
//! the label and weight builders of `WorkerConfig` went unseen.
//!
//! The guard extracts the four ```` ```rust ```` blocks: the activity
//! `requires`, the label map, `with_label` and `with_queue_weights`. It wraps
//! them with the imports and type stubs that the chapter implies, and
//! compiles the result. The macro copies the `requires` string without
//! parsing it. So the example also runs. It passes the string of the first
//! block to `eligibility::parse_requirements` and fails on a parse error.

use std::path::Path;
use std::process::ExitCode;

use smop::prelude::*;

use super::default_run::support::{self, Report, ScratchExample, SnippetCompile};
use crate::util;

const EXAMPLE_NAME: &str = "_onramp_doc_check_worker_routing";
const CH9: &str = "docs/getting-started/09-worker-routing.md";

/// Arguments of `cargo xtask check worker-routing-examples`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// Runs `cargo xtask check worker-routing-examples`.
pub fn run(_args: Args) -> Result<ExitCode> {
    let root = util::repo_root();
    let example = ScratchExample::claim(
        root.join(format!("autumn-harvest-plugin/examples/{EXAMPLE_NAME}.rs")),
    );
    let report = match render(&root) {
        Ok(source) => SnippetCompile {
            cargo_args: &[
                "run",
                "--quiet",
                "-p",
                "autumn-harvest-plugin",
                "--example",
                EXAMPLE_NAME,
            ],
            ok_line: "OK: chapter 9 snippets compile and the requires string parses.",
            failure_head: "chapter 9 snippets fail",
            count_noun: "compile error(s)",
            error_prefix: "error[",
        }
        .run(&root, &example, &source)?,
        Err(report) => report,
    };
    Ok(report.emit())
}

/// Builds the example source from chapter 9 under `root`.
///
/// An `Err` holds the report of a chapter with fewer than four blocks.
#[allow(
    clippy::literal_string_with_formatting_args,
    reason = "the braces belong to the generated example, which formats them itself"
)]
fn render(root: &Path) -> std::result::Result<String, Report> {
    let doc = support::read_or_empty(&root.join(CH9));
    let blocks: Vec<String> = (1..=4).map(|n| support::rust_block(&doc, n)).collect();
    if blocks.iter().any(String::is_empty) {
        let mut report = Report::new();
        report.fail(format!(
            "could not extract the four rust blocks of {CH9}; has the chapter been restructured? Update this guard."
        ));
        return Err(report);
    }
    let weights = format!("let _ = {};", blocks[3]);
    Ok(support::echo_lines(&[
        "#![allow(dead_code, unused)]",
        "use autumn_harvest::prelude::*;",
        "use serde::{Deserialize, Serialize};",
        "#[derive(Serialize, Deserialize)] struct TranscodeInput { url: String }",
        "#[derive(Serialize, Deserialize, Default)] struct TranscodeOutput { bytes: u64 }",
        "mod activity_block { use super::*;",
        &blocks[0],
        "}",
        "fn labels_map() {",
        &blocks[1],
        "}",
        "fn labels_single() {",
        &blocks[2],
        "}",
        "fn weights() {",
        &weights,
        "}",
        "fn main() {",
        "    let requires = activity_block::transcode_video_info().requires",
        "        .expect(\"chapter 9 activity declares no requires\");",
        "    let parsed = autumn_harvest::eligibility::parse_requirements(requires)",
        "        .unwrap_or_else(|e| panic!(\"chapter 9 requires {requires:?} does not parse: {e}\"));",
        "    assert!(!parsed.is_empty());",
        "}",
    ]))
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use super::*;

    fn fixture(blocks: usize) -> smop::fs::TempDir {
        let dir = fs::temp_dir().expect("temp dir");
        let doc = dir.path().join(CH9);
        std::fs::create_dir_all(doc.parent().expect("parent")).expect("mkdir");
        let mut text = String::new();
        for n in 1..=blocks {
            write!(text, "```rust\nblock{n}\n```\n").expect("write");
        }
        std::fs::write(doc, text).expect("doc");
        dir
    }

    #[test]
    fn render_wraps_each_block_in_its_slot() {
        let dir = fixture(4);
        let source = render(dir.path()).expect("render");
        assert!(source.contains("mod activity_block { use super::*;\nblock1\n}\nfn labels_map() {\nblock2\n}\nfn labels_single() {\nblock3\n}\nfn weights() {\nlet _ = block4;\n}\n"));
        assert!(source.ends_with("    assert!(!parsed.is_empty());\n}\n"));
    }

    #[test]
    fn three_blocks_are_not_enough() {
        let dir = fixture(3);
        let report = render(dir.path()).expect_err("too few");
        assert_eq!(report.code(), 1);
        assert!(
            report
                .stderr()
                .starts_with("could not extract the four rust blocks of")
        );
    }
}
