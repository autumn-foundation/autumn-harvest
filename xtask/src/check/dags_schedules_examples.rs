//! `cargo xtask check dags-schedules-examples`: the port of
//! `scripts/check-dags-schedules-examples.sh`.
//!
//! The guard fails if the snippets of chapter 8 (DAGs and schedules) stop
//! compiling against the real crate.
//!
//! Mechanism this guards against: the chapter shows 15 ```` ```rust ````
//! blocks that a reader pastes verbatim, and only a syntax check covers them.
//! A renamed `DagBuilder` method or trigger rule goes unseen until a reader
//! pastes the block.
//!
//! Each block gets its own module with the imports and stubs that the prose
//! implies. Blocks 3, 8 and 9 are fragments. The guard wraps them in a
//! function with the stubs that they name. Blocks 11 and 14 are not
//! compiled: they need a `refund` helper and a full plugin host.

use std::path::Path;
use std::process::ExitCode;

use smop::prelude::*;

use super::support::{self, Report, ScratchExample, SnippetCompile};
use crate::util;

const EXAMPLE_NAME: &str = "_onramp_doc_check_dags_schedules";
const CH8: &str = "docs/getting-started/08-dags-and-schedules.md";
const PRE: &str = "use autumn_harvest::prelude::*;";

/// Arguments of `cargo xtask check dags-schedules-examples`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// Runs `cargo xtask check dags-schedules-examples`.
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
            ok_line: "OK: chapter 8 snippets compile.",
            failure_head: "chapter 8 snippets fail",
            count_noun: "compile error(s)",
            error_prefix: "error",
        }
        .run(&root, &example, &source)?,
        Err(report) => report,
    };
    Ok(report.emit())
}

/// Returns one activity stub, without a trailing newline.
///
/// The old guard joined several `$(stub x)` substitutions on one line, so
/// the stubs of one module share a line.
fn stub(name: &str) -> String {
    format!(
        "#[activity(start_to_close = \"30s\")] async fn {name}(_ctx: &ActivityContext, _i: Value) -> HarvestResult<Value> {{ Ok(Value::Null) }}"
    )
}

/// Returns the stubs of `names`, joined on one line.
fn stubs(names: &[&str]) -> String {
    names.iter().map(|name| stub(name)).collect()
}

/// Wraps one body in a module.
///
/// The module adds the two imports that the chapter leaves implicit, unless
/// the block already has them.
fn module(name: &str, body: &str) -> String {
    let mut extra = String::new();
    if !body.contains("use serde_json::Value;") {
        extra.push_str("use serde_json::Value; ");
    }
    if !body.contains("use std::time::Duration;") {
        extra.push_str("use std::time::Duration; ");
    }
    format!("mod {name} {{\n#![allow(dead_code, unused)]\n{PRE}\n{extra}\n{body}\n}}\n")
}

/// Builds the example source from chapter 8 under `root`.
///
/// An `Err` holds the report of a chapter whose block count changed.
fn render(root: &Path) -> std::result::Result<String, Report> {
    let doc = support::read_or_empty(&root.join(CH8));
    let total = support::lines(&doc)
        .into_iter()
        .filter(|line| *line == "```rust")
        .count();
    if total != 15 {
        let mut report = Report::new();
        report.fail(format!(
            "{CH8} has {total} rust blocks, expected 15; update this guard to cover the new layout."
        ));
        return Err(report);
    }
    let block = |n: usize| support::rust_block(&doc, n);

    let mut source = String::from("#![allow(dead_code, unused)]\n");
    source.push_str(&module("b1", &block(1)));
    source.push_str(&module(
        "b2",
        &format!(
            "{}\n#[dag] pub fn frag(dag: &mut DagBuilder) {{\nlet list = dag.activity(list_partitions);\n{};\n}}",
            block(2),
            block(3)
        ),
    ));
    source.push_str(&module(
        "b4",
        &format!(
            "{}\n{}",
            stubs(&["extract_users", "load_warehouse"]),
            block(4)
        ),
    ));
    source.push_str(&module(
        "b5",
        &format!(
            "{}\n{}\n{}",
            block(5),
            stubs(&["low_risk_path", "medium_risk_path", "high_risk_path"]),
            block(6)
        ),
    ));
    source.push_str(&module("b7", &block(7)));
    source.push_str(&module(
        "b8",
        &format!(
            "{}\n#[dag] pub fn frag(dag: &mut DagBuilder) {{\n{}\n}}",
            stubs(&["fetch_users", "fetch_orders", "join_report"]),
            block(8)
        ),
    ));
    source.push_str(&module(
        "b9",
        &format!(
            "{}\n#[dag] pub fn frag(dag: &mut DagBuilder) {{\nlet root = dag.activity(root_step);\n{}\n}}",
            stubs(&["root_step", "maybe_step", "finalize"]),
            block(9)
        ),
    ));
    source.push_str(&module(
        "b10",
        &format!(
            "{}\n{}\n{}",
            stubs(&[
                "reserve_inventory",
                "release_inventory",
                "charge_payment",
                "refund_payment"
            ]),
            stubs(&[
                "allocate_shipment",
                "deallocate_shipment",
                "print_label",
                "void_label",
                "notify_customer"
            ]),
            block(10)
        ),
    ));
    source.push_str(&module(
        "b12",
        &format!("{}\n{}", stubs(&["extract_order", "load_order"]), block(12)),
    ));
    source.push_str(&module(
        "b13",
        &format!(
            "{}\nuse autumn_harvest::dag::GateTimeoutAction;\n{}",
            stubs(&["extract_order", "load_order", "escalate_to_manager"]),
            block(13)
        ),
    ));
    source.push_str(&module("b15", &block(15)));
    source.push_str("fn main() {}\n");
    Ok(source)
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use super::*;

    fn fixture(blocks: usize) -> smop::fs::TempDir {
        let dir = fs::temp_dir().expect("temp dir");
        let doc = dir.path().join(CH8);
        std::fs::create_dir_all(doc.parent().expect("parent")).expect("mkdir");
        let mut text = String::new();
        for n in 1..=blocks {
            write!(text, "Prose {n}.\n\n```rust\nblock_{n}();\n```\n\n").expect("write");
        }
        std::fs::write(doc, text).expect("doc");
        dir
    }

    #[test]
    fn render_skips_blocks_11_and_14_and_wraps_the_fragments() {
        let dir = fixture(15);
        let source = render(dir.path()).expect("render");
        assert!(!source.contains("block_11"));
        assert!(!source.contains("block_14"));
        assert!(source.contains(
            "block_2();\n#[dag] pub fn frag(dag: &mut DagBuilder) {\nlet list = dag.activity(list_partitions);\nblock_3();;\n}\n}\n"
        ));
        assert!(source.starts_with(
            "#![allow(dead_code, unused)]\nmod b1 {\n#![allow(dead_code, unused)]\nuse autumn_harvest::prelude::*;\nuse serde_json::Value; use std::time::Duration; \nblock_1();\n}\n"
        ));
        assert!(source.ends_with("block_15();\n}\nfn main() {}\n"));
    }

    #[test]
    fn module_skips_an_import_that_the_block_has() {
        let wrapped = module("m", "use serde_json::Value;\nx");
        assert!(wrapped.contains("\nuse std::time::Duration; \nuse serde_json::Value;\nx\n}\n"));
    }

    #[test]
    fn a_changed_block_count_fails() {
        let dir = fixture(14);
        let report = render(dir.path()).expect_err("count");
        assert_eq!(
            report.stderr(),
            format!(
                "{CH8} has 14 rust blocks, expected 15; update this guard to cover the new layout.\n"
            )
        );
    }
}
