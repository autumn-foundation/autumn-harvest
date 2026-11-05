//! `cargo xtask check broker-connector-cutover-example`: the port of
//! `scripts/check-broker-connector-cutover-example.sh`.
//!
//! The guard fails if the cutover example of
//! `docs/getting-started/13-broker-connectors.md` drifts from its compiled
//! copy in `autumn-harvest-plugin/src/connector/binding.rs`.
//!
//! Mechanism this guards against: one section of the chapter is "Cutting a
//! binding over to a new cluster or a recreated topic". Its example called
//! `.map_json(...)` with the wrong closure signature (PR #1523). A shell approximation of
//! "does this closure type-check" kept acquiring new gaps. It checked arity,
//! parameter types, return type and tail position, and each patch left
//! another gap open. A markdown fence is never compiled anywhere else. No
//! bound limits how many ways a hand-rolled pattern match can be wrong about
//! Rust. A real compiler has no such gap.
//!
//! The fix: the example in the doc must be byte-identical to its compiled
//! copy, module indentation aside. The copy is the block between the
//! `cutover-example-start` and `cutover-example-end` markers in
//! `cutover_example_from_getting_started_ch13_type_checks`. That test is an
//! ordinary test in `autumn-harvest-plugin/src/connector/binding.rs`. CI's
//! `connector` lib-test job compiles and runs it with
//! `cargo test -p autumn-harvest-plugin --features connectors --lib connector`.
//! So a real compile enforces the correctness of the example. The only job
//! of this guard is to catch the doc and the compiled copy drifting apart.

use std::path::Path;
use std::process::ExitCode;

use regex::Regex;
use smop::prelude::*;

use super::default_run::support::Report;
use crate::util;

const DOC: &str = "docs/getting-started/13-broker-connectors.md";
const SRC: &str = "autumn-harvest-plugin/src/connector/binding.rs";
const ANCHOR: &str = "SourceBinding::starts(\"orders\", \"orders\", \"order_flow\")";

/// Arguments of `cargo xtask check broker-connector-cutover-example`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// Runs `cargo xtask check broker-connector-cutover-example`.
pub fn run(_args: Args) -> Result<ExitCode> {
    Ok(evaluate(&util::repo_root())?.emit())
}

/// Reads a file the way Python's `open()` does in text mode.
///
/// Universal newlines turn `\r\n` and a lone `\r` into `\n`. The old guard
/// read each file in a Python helper. A file that did not decode as UTF-8
/// made that helper fail and print nothing. The guard then reported its
/// normal "could not find" finding with exit code 1. So an unreadable file
/// gives an empty text here, and the caller reports that same finding.
fn read_text(path: &Path) -> String {
    std::fs::read(path)
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .map(|text| text.replace("\r\n", "\n").replace('\r', "\n"))
        .unwrap_or_default()
}

/// Returns the whole body of the ```` ```rust ```` fence that holds the anchor.
///
/// The window runs from right after the opening fence to right before the
/// closing fence. It does not start at the anchor line. A window that starts
/// at the anchor ignores a bad `let` inserted between the fence and the
/// anchor. That line is inside the compiled fence of the example, so the
/// comparison must see it too.
fn doc_block(text: &str) -> Result<String> {
    let fence = Regex::new(r"(?s)```rust\n(.*?)\n```")?;
    let found = fence
        .captures_iter(text)
        .filter_map(|caps| caps.get(1))
        .map(|body| body.as_str())
        .find(|body| body.contains(ANCHOR))
        .unwrap_or_default();
    Ok(found.trim_end_matches('\n').to_owned())
}

/// Returns the compiled copy between the markers, dedented.
///
/// The dedent strips the leading whitespace that every non-blank line shares.
/// The test nests the copy in a fn, a `let` and a block. That extra
/// indentation must not count as a difference from the doc, which starts at
/// column 0.
fn src_block(text: &str) -> Result<String> {
    let markers = Regex::new(r"(?s)// cutover-example-start\n(.*?)\n[ \t]*// cutover-example-end")?;
    let Some(body) = markers.captures(text).and_then(|caps| caps.get(1)) else {
        return Ok(String::new());
    };
    let lines: Vec<&str> = body.as_str().split('\n').collect();
    let strip = lines
        .iter()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.chars().count() - line.trim_start_matches([' ', '\t']).chars().count())
        .min()
        .unwrap_or(0);
    let dedented: Vec<String> = lines
        .iter()
        .map(|line| {
            if line.chars().count() >= strip {
                line.chars().skip(strip).collect()
            } else {
                (*line).to_owned()
            }
        })
        .collect();
    Ok(dedented.join("\n").trim_end_matches('\n').to_owned())
}

/// Compares the doc example under `root` with its compiled copy.
fn evaluate(root: &Path) -> Result<Report> {
    let mut report = Report::new();
    for file in [DOC, SRC] {
        if !root.join(file).is_file() {
            report.fail(format!("{file}: not found"));
            return Ok(report);
        }
    }

    let doc = doc_block(&read_text(&root.join(DOC)))?;
    if doc.is_empty() {
        report.fail(format!(
            "{DOC}: could not find the cutover example's {ANCHOR} call; has the chapter been restructured? Update this guard (and the compiled copy in {SRC}) to match."
        ));
        return Ok(report);
    }
    if !doc.contains("key_incarnation(\"2026-08-cutover\")") {
        report.fail(format!(
            "{DOC}: found {ANCHOR} but not .key_incarnation(\"2026-08-cutover\") before the fence closes; has the cutover example moved or changed shape? Update this guard (and the compiled copy in {SRC}) to match."
        ));
        return Ok(report);
    }

    let src = src_block(&read_text(&root.join(SRC)))?;
    if src.is_empty() {
        report.fail(format!(
            "{SRC}: could not find the // cutover-example-start ... // cutover-example-end markers in cutover_example_from_getting_started_ch13_type_checks; has the test moved or been renamed? Update this guard to match."
        ));
        return Ok(report);
    }

    if doc != src {
        report.err(format!(
            "{DOC}'s cutover example has drifted from the compiled copy in {SRC} (between the cutover-example-start/end markers in cutover_example_from_getting_started_ch13_type_checks). Only the compiled copy is verified to type-check, so the two must read identically (module indentation aside)."
        ));
        report.err("");
        report.err(format!("--- {DOC} ---"));
        report.err(doc);
        report.err("");
        report.err(format!("--- {SRC} (dedented) ---"));
        report.err(src);
        report.err("");
        report.fail(
            "Fix: make one match the other, then re-run 'cargo test -p autumn-harvest-plugin --features connectors --lib connector::binding::tests::cutover_example_from_getting_started_ch13_type_checks' to confirm it still compiles.",
        );
        return Ok(report);
    }

    report.out(format!(
        "OK: the cutover example matches its compiled copy in {SRC}."
    ));
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE: &str = "SourceBinding::starts(\"orders\", \"orders\", \"order_flow\")\n    .key_incarnation(\"2026-08-cutover\")";

    fn fixture(doc_body: &str, src_body: &str) -> smop::fs::TempDir {
        let dir = fs::temp_dir().expect("temp dir");
        let doc = dir.path().join(DOC);
        let src = dir.path().join(SRC);
        std::fs::create_dir_all(doc.parent().expect("parent")).expect("mkdir");
        std::fs::create_dir_all(src.parent().expect("parent")).expect("mkdir");
        std::fs::write(&doc, format!("# Ch13\n\n```rust\n{doc_body}\n```\n")).expect("doc");
        let indented: Vec<String> = src_body.lines().map(|l| format!("        {l}")).collect();
        std::fs::write(
            &src,
            format!(
                "fn t() {{\n        // cutover-example-start\n{}\n        // cutover-example-end\n}}\n",
                indented.join("\n")
            ),
        )
        .expect("src");
        dir
    }

    #[test]
    fn identical_copies_pass() {
        let dir = fixture(EXAMPLE, EXAMPLE);
        let report = evaluate(dir.path()).expect("evaluate");
        assert_eq!(report.code(), 0, "{}", report.stderr());
    }

    #[test]
    fn a_line_above_the_anchor_is_drift() {
        let dir = fixture(&format!("let bad = 1;\n{EXAMPLE}"), EXAMPLE);
        let report = evaluate(dir.path()).expect("evaluate");
        assert_eq!(report.code(), 1);
        assert!(report.stderr().contains(
            "--- autumn-harvest-plugin/src/connector/binding.rs (dedented) ---\nSourceBinding"
        ));
    }

    #[test]
    fn a_missing_incarnation_is_reported() {
        let dir = fixture(
            "SourceBinding::starts(\"orders\", \"orders\", \"order_flow\")",
            EXAMPLE,
        );
        let report = evaluate(dir.path()).expect("evaluate");
        assert_eq!(report.code(), 1);
        assert!(report.stderr().contains("but not .key_incarnation"));
    }

    #[test]
    fn src_block_dedents_by_the_common_indent_only() {
        let text = "  // cutover-example-start\n    a\n\n      b\n  // cutover-example-end\n";
        assert_eq!(src_block(text).expect("src"), "a\n\n  b");
    }

    #[test]
    fn a_crlf_doc_still_matches() {
        let dir = fixture(EXAMPLE, EXAMPLE);
        let doc = dir.path().join(DOC);
        let crlf = std::fs::read_to_string(&doc)
            .expect("doc")
            .replace('\n', "\r\n");
        std::fs::write(&doc, crlf).expect("crlf");
        let report = evaluate(dir.path()).expect("evaluate");
        assert_eq!(report.code(), 0, "{}", report.stderr());
    }

    #[test]
    fn a_file_that_is_not_utf8_is_a_finding_not_an_error() {
        let dir = fixture(EXAMPLE, EXAMPLE);
        let doc = dir.path().join(DOC);
        let mut bytes = std::fs::read(&doc).expect("doc");
        bytes.extend_from_slice(b"\xff\n");
        std::fs::write(&doc, bytes).expect("bad doc");
        let report = evaluate(dir.path()).expect("evaluate");
        assert_eq!(report.code(), 1);
        assert!(
            report
                .stderr()
                .contains("could not find the cutover example's")
        );

        let dir = fixture(EXAMPLE, EXAMPLE);
        let src = dir.path().join(SRC);
        let mut bytes = std::fs::read(&src).expect("src");
        bytes.extend_from_slice(b"// \xff\n");
        std::fs::write(&src, bytes).expect("bad src");
        let report = evaluate(dir.path()).expect("evaluate");
        assert_eq!(report.code(), 1);
        assert!(
            report
                .stderr()
                .contains("could not find the // cutover-example-start")
        );
    }
}
