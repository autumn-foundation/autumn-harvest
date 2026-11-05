//! `cargo xtask check webhooks-feature-example`: the port of
//! `scripts/check-webhooks-feature-example.sh`.
//!
//! The guard fails if the webhook example of chapter 12 regresses in one of
//! two ways:
//!
//! 1. Doc drift: the "Enable the feature" note disappears from the chapter,
//!    or moves past step 3. An Onramp clean-room pass added that note. It
//!    tells the reader that `HarvestPlugin::webhooks(...)` needs the
//!    `webhooks` Cargo feature of `autumn-harvest-plugin`.
//! 2. Code drift: the `#[webhook]` mapping function of step 2 and the
//!    `HarvestPlugin` wiring of step 3 stop compiling against the real crate.
//!
//! Mechanism this guards against: chapter 12 showed the `#[webhook]` mapping
//! function and the `.webhooks(webhooks![...])` plugin wiring. It did not say
//! that `autumn-harvest-plugin` needs `features = ["webhooks"]`. The sibling
//! chapter 13 (broker connectors) states its `connectors` feature up front in
//! its own "Features:" table. Take a newcomer who scaffolds a project per
//! chapter 1, with no extra features. That newcomer pastes the code of chapter
//! 12 verbatim and gets this error at the `.webhooks(...)` call:
//!
//! ```text
//! error[E0599]: no method named `webhooks` found for struct `HarvestPlugin`
//! ```
//!
//! The message names neither "webhooks" nor chapter 12. A live run
//! reproduced it. The compile check of this guard fails with exactly that
//! error under the default features. It succeeds once `--features webhooks`
//! is added. That confirms the feature as the fix, not a coincidence.

use std::path::Path;
use std::process::ExitCode;

use smop::prelude::*;

use super::support::{self, Report, ScratchExample};
use crate::util;

const DOC: &str = "docs/getting-started/12-webhooks.md";
const EXAMPLE_NAME: &str = "_onramp_doc_check_webhooks_feature";
const STEP2: &str = "## 2. Write the mapping function";
const STEP3: &str = "## 3. Wire the plugin";

/// Arguments of `cargo xtask check webhooks-feature-example`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// Runs `cargo xtask check webhooks-feature-example`.
pub fn run(_args: Args) -> Result<ExitCode> {
    let root = util::repo_root();
    let example = ScratchExample::claim(
        root.join(format!("autumn-harvest-plugin/examples/{EXAMPLE_NAME}.rs")),
    );
    let report = evaluate(&root, |source, features| {
        example.write(source)?;
        let mut args = vec![
            "check",
            "--quiet",
            "-p",
            "autumn-harvest-plugin",
            "--example",
            EXAMPLE_NAME,
        ];
        if !features.is_empty() {
            args.extend(["--features", features]);
        }
        support::cargo_merged(&root, &args)
    })?;
    Ok(report.emit())
}

/// Returns the lines before the step 3 heading, or the whole doc without it.
fn before_step3(doc: &str) -> String {
    let body: Vec<&str> = support::lines(doc)
        .into_iter()
        .take_while(|line| *line != STEP3)
        .collect();
    support::join_captured(&body)
}

/// Returns the ```` ```rust ```` body of the section under `heading`.
///
/// This copies two chained awk calls. The first takes the lines after the
/// heading up to the first ```` ``` ```` line. The second keeps the lines
/// after the first ```` ```rust ```` line of that window.
fn section_rust(doc: &str, heading: &str) -> String {
    let mut inside = false;
    let mut window = Vec::new();
    for line in support::lines(doc) {
        if line == heading {
            inside = true;
            continue;
        }
        if inside && line == "```" {
            break;
        }
        if inside {
            window.push(line);
        }
    }
    let mut printing = false;
    let mut body = Vec::new();
    for line in window {
        if line == "```rust" {
            printing = true;
            continue;
        }
        if printing {
            body.push(line);
        }
    }
    support::join_captured(&body)
}

/// Builds the example source from the two snippets.
fn render(mapping_fn: &str, plugin_wiring: &str) -> String {
    support::echo_lines(&[
        "#![allow(clippy::all, clippy::pedantic, clippy::nursery, dead_code)]",
        "use autumn_harvest_plugin::HarvestPlugin;",
        mapping_fn,
        "",
        "#[workflow]",
        "async fn subscription_flow(_ctx: &WorkflowContext, _input: String) -> HarvestResult<String> {",
        "    Ok(\"done\".into())",
        "}",
        "",
        "#[autumn_web::main]",
        "async fn main() {",
        plugin_wiring,
        "    app.run().await;",
        "}",
    ])
}

/// Checks the chapter under `root`.
///
/// `compile` writes one source and checks it with the named features.
fn evaluate(
    root: &Path,
    mut compile: impl FnMut(&str, &str) -> Result<(bool, String)>,
) -> Result<Report> {
    let mut report = Report::new();
    let path = root.join(DOC);
    if !path.is_file() {
        report.fail(format!("{DOC}: not found"));
        return Ok(report);
    }
    let doc = support::read_or_empty(&path);

    // Part 1, doc drift: the feature note must exist and precede step 3.
    let head = before_step3(&doc);
    if head.is_empty() {
        report.fail(format!(
            "{DOC}: could not find '{STEP3}'; has the chapter been restructured? Update this guard to match."
        ));
    } else if !head.contains("features = [\"webhooks\"]") {
        report.fail(format!(
            "{DOC}: no mention of `features = [\"webhooks\"]` before step 3. Without it, a reader who scaffolds per chapter 1 and pastes this chapter's code verbatim hits 'no method named `webhooks` found for struct `HarvestPlugin`' at the .webhooks(...) call, with nothing naming the missing feature. See the 'Enable the feature' section this guard expects, and chapter 13's 'Features:' table for the sibling convention."
        ));
    }

    // Part 2, code drift: the snippets of the doc must still compile.
    let mapping_fn = section_rust(&doc, STEP2);
    let plugin_wiring = section_rust(&doc, STEP3);
    if mapping_fn.is_empty() || plugin_wiring.is_empty() {
        report.fail(format!(
            "{DOC}: could not extract the step-2 mapping-function block or the step-3 plugin-wiring block; has the chapter been restructured? Update this guard to match."
        ));
        return Ok(report);
    }

    let source = render(&mapping_fn, &plugin_wiring);
    for (features, expect_pass) in [("", false), ("webhooks", true)] {
        let (passed, output) = compile(&source, features)?;
        if passed && !expect_pass {
            report.fail(format!(
                "{DOC}'s webhook example compiled WITHOUT --features webhooks, but this guard expects it to fail there (that's the whole point of the 'Enable the feature' note). Either the feature gate moved, or this guard is stale -- update whichever is now wrong."
            ));
        } else if !passed && expect_pass {
            let shown = if features.is_empty() {
                "<default>"
            } else {
                features
            };
            report.err(format!(
                "{DOC}'s webhook example does not compile against the real autumn-harvest-plugin crate (features: '{shown}')."
            ));
            report.err("");
            report.fail(output);
        }
    }

    if report.code() == 0 {
        report.out(
            "OK: chapter 12's webhook example compiles with --features webhooks, fails to compile without it as documented, and the feature is documented before step 3.",
        );
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    const GOOD: &str = "# Webhooks\n\n## Enable the feature\n\n```toml\nautumn-harvest-plugin = { features = [\"webhooks\"] }\n```\n\n## 2. Write the mapping function\n\n```rust\n#[webhook]\nfn map() {}\n```\n\n## 3. Wire the plugin\n\n```rust\nlet app = autumn_web::app();\n```\n";

    fn fixture(body: &str) -> smop::fs::TempDir {
        let dir = fs::temp_dir().expect("temp dir");
        let doc = dir.path().join(DOC);
        std::fs::create_dir_all(doc.parent().expect("parent")).expect("mkdir");
        std::fs::write(doc, body).expect("doc");
        dir
    }

    #[test]
    fn the_gate_holds_only_with_the_feature() {
        let dir = fixture(GOOD);
        let mut calls = Vec::new();
        let report = evaluate(dir.path(), |source, features| {
            calls.push(features.to_owned());
            assert!(source.contains("use autumn_harvest_plugin::HarvestPlugin;\n#[webhook]\nfn map() {}\n\n#[workflow]\n"));
            assert!(source.ends_with("async fn main() {\nlet app = autumn_web::app();\n    app.run().await;\n}\n"));
            Ok((features == "webhooks", "error[E0599]".into()))
        })
        .expect("evaluate");
        assert_eq!(report.code(), 0, "{}", report.stderr());
        assert_eq!(calls, ["", "webhooks"]);
    }

    #[test]
    fn a_compile_without_the_feature_is_a_stale_gate() {
        let dir = fixture(GOOD);
        let report = evaluate(dir.path(), |_, _| Ok((true, String::new()))).expect("evaluate");
        assert_eq!(report.code(), 1);
        assert!(
            report
                .stderr()
                .contains("compiled WITHOUT --features webhooks")
        );
    }

    #[test]
    fn a_failed_compile_with_the_feature_prints_the_output() {
        let dir = fixture(GOOD);
        let report =
            evaluate(dir.path(), |_, _| Ok((false, "error[E1]: x".into()))).expect("evaluate");
        assert!(
            report
                .stderr()
                .ends_with("(features: 'webhooks').\n\nerror[E1]: x\n")
        );
    }

    #[test]
    fn a_note_after_step_3_fails_but_still_compiles() {
        let late = GOOD.replace("features = [\"webhooks\"]", "x") + "\nfeatures = [\"webhooks\"]\n";
        let dir = fixture(&late);
        let mut compiled = 0;
        let report = evaluate(dir.path(), |_, features| {
            compiled += 1;
            Ok((features == "webhooks", String::new()))
        })
        .expect("evaluate");
        assert_eq!(report.code(), 1);
        assert_eq!(compiled, 2);
        assert!(
            report
                .stderr()
                .contains("no mention of `features = [\"webhooks\"]` before step 3.")
        );
    }

    #[test]
    fn a_missing_snippet_stops_before_compiling() {
        let dir = fixture("features = [\"webhooks\"]\n## 3. Wire the plugin\n```rust\nx\n```\n");
        let report = evaluate(dir.path(), |_, _| panic!("no compile")).expect("evaluate");
        assert_eq!(report.code(), 1);
        assert!(
            report
                .stderr()
                .contains("could not extract the step-2 mapping-function block")
        );
    }
}
