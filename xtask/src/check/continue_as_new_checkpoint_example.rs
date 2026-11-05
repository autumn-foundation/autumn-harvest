//! `cargo xtask check continue-as-new-checkpoint-example`: the port of
//! `scripts/check-continue-as-new-checkpoint-example.sh`.
//!
//! The guard fails if either copy of the "checkpoint before the
//! `execution_timeout` deadline" `continue_as_new` example regresses into a
//! real type error. The defect is a `ctx.continue_as_new(...).await` whose
//! error is not converted to `String` before the `?` of the surrounding
//! `Result<_, String>` workflow.
//!
//! Mechanism: `WorkflowContext::continue_as_new` returns `HarvestResult<()>`,
//! which is `Result<(), HarvestError>`. There is no
//! `From<HarvestError> for String`. So `?` needs a real conversion first, or
//! rustc reports E0277. An Onramp clean-room pass found this in the flagship
//! deadline-aware-checkpoint snippet of
//! `docs/getting-started/07-reliability-knobs.md`. The doc-snippet-syntax
//! audit never catches it, because that audit only runs `rustfmt`. A syntax
//! check does not see a type error. The identical broken snippet also lived,
//! unenforced, in the module-doc comment of
//! `autumn-harvest/examples/long_lived_entity_deadline.rs`. It was marked
//! `rust,ignore`, so nothing compiled it either.
//!
//! This guard compiles each pinned snippet with `cargo check`. It does not
//! pattern-match stand-in text. PR #1700 found text heuristics gameable here
//! twice. The first heuristic was a loose window that an unrelated
//! `.map_err(` could satisfy. The second accepted `.await.map_err(|e| e)?`, an
//! identity closure. That text is "a `map_err` right after await", but
//! `|e| e` keeps `HarvestError` and does not produce `String`. So it still
//! fails to compile.
//!
//! Whether the return type of a closure satisfies the target of `?` is a
//! property of the type checker, not a text shape. So no third regex is worth
//! writing here, because the compiler already settles it.
//! `cargo xtask audit doc-snippet-syntax` deliberately does not compile the
//! whole corpus of about 93 blocks, and its own module doc explains why. That
//! tradeoff does not apply to two pinned blocks that were broken before. The
//! same `lint` job already builds the crate for clippy a few steps later.

use std::path::Path;
use std::process::ExitCode;

use smop::prelude::*;

use super::support::{self, Report, ScratchExample};
use crate::util;

const EXAMPLE_NAME: &str = "_onramp_doc_check_continue_as_new_checkpoint";
const DOC: &str = "docs/getting-started/07-reliability-knobs.md";
const RS: &str = "autumn-harvest/examples/long_lived_entity_deadline.rs";

/// The harness that precedes each snippet.
///
/// Neither pinned snippet defines `SubState` or a `use` of its own. The doc
/// assumes context from the chapter. The `use autumn_harvest::prelude::*;`
/// line of the `.rs` snippet repeats the one here, and an identical repeated
/// glob import is not an error. `SubState` matches the real one in
/// `autumn-harvest/examples/long_lived_entity_deadline.rs`.
const HARNESS_PRELUDE: &str = "#![allow(clippy::all, clippy::pedantic, clippy::nursery, dead_code)]
use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SubState {
    pub cycles: u32,
}
";

/// Arguments of `cargo xtask check continue-as-new-checkpoint-example`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// Runs `cargo xtask check continue-as-new-checkpoint-example`.
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

/// Returns the checkpoint example of the chapter.
///
/// The window runs from its `#[workflow(execution_timeout = "24h")]` line to
/// the close of the fence.
fn extract_md_block(doc: &str) -> String {
    support::window_from(doc, "#[workflow(execution_timeout = \"24h\")]")
}

/// Returns the `rust,ignore` illustration of the module doc, without `//! `.
fn extract_rs_doc_comment_block(source: &str) -> String {
    let mut printing = false;
    let mut body = Vec::new();
    for line in support::lines(source) {
        // The awk rule on the opening line also skips a repeat of that line.
        if line == "//! ```rust,ignore" {
            printing = true;
            continue;
        }
        if !printing {
            continue;
        }
        if line == "//! ```" {
            break;
        }
        let stripped = line
            .strip_prefix("//!")
            .map_or(line, |rest| rest.strip_prefix(' ').unwrap_or(rest));
        body.push(stripped);
    }
    support::join_captured(&body)
}

/// Builds the example source around one snippet.
fn render(code: &str) -> String {
    let mut source = HARNESS_PRELUDE.to_owned();
    source.push_str(&support::echo_lines(&[
        "",
        code,
        "",
        "fn main() {",
        "    let _wfs = workflows![subscription_entity];",
        "}",
    ]));
    source
}

/// Checks both copies under `root`. `compile` writes and checks one source.
fn evaluate(
    root: &Path,
    mut compile: impl FnMut(&str) -> Result<(bool, String)>,
) -> Result<Report> {
    let mut report = Report::new();
    let mut check = |report: &mut Report, label: &str, code: &str| -> Result<()> {
        let (passed, output) = compile(&render(code))?;
        if !passed {
            report.err(format!(
                "{label} does not compile against the real autumn-harvest crate."
            ));
            report.err("");
            report.err(output);
            report.err("");
            report.fail(
                "See the real, compiling subscription_entity fn in autumn-harvest/examples/long_lived_entity_deadline.rs.",
            );
        }
        Ok(())
    };

    let md_code = extract_md_block(&support::read_or_empty(&root.join(DOC)));
    if md_code.is_empty() {
        report.fail(format!(
            "{DOC}: could not find the subscription_entity checkpoint example (looked for its #[workflow(execution_timeout = \"24h\")] attribute line); has the chapter been restructured? Update this guard to match."
        ));
    } else {
        check(
            &mut report,
            &format!("{DOC}'s checkpoint example"),
            &md_code,
        )?;
    }

    let rs_code = extract_rs_doc_comment_block(&support::read_or_empty(&root.join(RS)));
    if rs_code.is_empty() {
        report.fail(format!(
            "{RS}: could not find its ```rust,ignore``` module-doc illustration; has it moved? Update this guard to match."
        ));
    } else {
        check(
            &mut report,
            "long_lived_entity_deadline.rs's module-doc illustration",
            &rs_code,
        )?;
    }

    if report.code() == 0 {
        report.out(
            "OK: both continue_as_new checkpoint examples compile against the real autumn-harvest crate.",
        );
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MD: &str = "text\n```rust\n#[workflow(execution_timeout = \"24h\")]\nasync fn subscription_entity() {}\n```\n";
    const RS_SRC: &str = "//! Intro\n//! ```rust,ignore\n//! #[workflow]\n//!     indented\n//!\n//! ```\nfn main() {}\n";

    fn fixture(md: &str, rs: &str) -> smop::fs::TempDir {
        let dir = fs::temp_dir().expect("temp dir");
        for (path, body) in [(DOC, md), (RS, rs)] {
            let full = dir.path().join(path);
            std::fs::create_dir_all(full.parent().expect("parent")).expect("mkdir");
            std::fs::write(full, body).expect("write");
        }
        dir
    }

    #[test]
    fn the_doc_comment_block_loses_one_prefix_space() {
        assert_eq!(
            extract_rs_doc_comment_block(RS_SRC),
            "#[workflow]\n    indented"
        );
    }

    #[test]
    fn both_blocks_compile_and_pass() {
        let dir = fixture(MD, RS_SRC);
        let mut sources = Vec::new();
        let report = evaluate(dir.path(), |source| {
            sources.push(source.to_owned());
            Ok((true, String::new()))
        })
        .expect("evaluate");
        assert_eq!(report.code(), 0);
        assert_eq!(sources.len(), 2);
        assert!(sources[0].starts_with(HARNESS_PRELUDE));
        assert!(sources[0].ends_with(
            "\n#[workflow(execution_timeout = \"24h\")]\nasync fn subscription_entity() {}\n\nfn main() {\n    let _wfs = workflows![subscription_entity];\n}\n"
        ));
    }

    #[test]
    fn a_failed_compile_and_a_missing_block_both_count() {
        let dir = fixture("no example\n", RS_SRC);
        let report =
            evaluate(dir.path(), |_| Ok((false, "error[E0277]: x".to_owned()))).expect("evaluate");
        assert_eq!(report.code(), 1);
        let stderr = report.stderr();
        assert!(stderr.contains("could not find the subscription_entity checkpoint example"));
        assert!(stderr.contains(
            "long_lived_entity_deadline.rs's module-doc illustration does not compile against the real autumn-harvest crate.\n\nerror[E0277]: x\n\nSee the real"
        ));
        assert_eq!(report.stdout(), "");
    }
}
