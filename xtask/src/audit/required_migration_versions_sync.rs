//! `cargo xtask audit required-migration-versions-sync`: the port of
//! `docs/audits/required-migration-versions-sync.py`.
//!
//! It keeps the required-migration-versions clone class in step (issue
//! #2358).
//!
//! Two files each define `required_migration_versions()`:
//! `autumn-harvest-plugin/src/preflight.rs` and
//! `autumn-harvest-plugin/src/shard_health.rs`. The two bodies are
//! byte-identical. Two copies are below the rule-of-three merge bar, so they
//! stay linked, not merged.
//!
//! This command extracts each body by brace matching. It does no
//! normalization. It fails in four cases:
//! - The bodies differ.
//! - A copy is missing, not unique or unclosed.
//! - A body has a block comment or a raw string. The brace matcher cannot
//!   read them, so the command refuses them.
//! - The sibling comment that names this issue is gone.
//!
//! Usage: `cargo xtask audit required-migration-versions-sync [--self-test]`.
//! The brace matcher lives in `libpq_keyword_sync.rs`.

use std::process::ExitCode;
use std::sync::LazyLock;

use regex::Regex;
use smop::prelude::*;

use super::libpq_keyword_sync::{
    UNSUPPORTED, file_name, find_fn, py_read_repo_file, py_splitlines,
};
use super::vantage_flash_live_region::compile;

/// Arguments of `cargo xtask audit required-migration-versions-sync`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {
    /// Run the fixtures instead of the real files.
    #[arg(long)]
    pub self_test: bool,
}

const ISSUE: &str = "#2358";
const FN_NAME: &str = "required_migration_versions";
static ISSUE_RE: LazyLock<Regex> =
    LazyLock::new(|| compile(&format!(r"{}\b", regex::escape(ISSUE))));
static SIGNATURE: LazyLock<Regex> = LazyLock::new(|| {
    compile(&format!(
        r"(?m)^fn {FN_NAME}\(\) -> Result<Vec<String>, String> \{{"
    ))
});

const TARGETS: [&str; 2] = [
    "autumn-harvest-plugin/src/preflight.rs",
    "autumn-harvest-plugin/src/shard_health.rs",
];

/// Returns true when the comment block just above `start` names this issue.
///
/// The block must also name the `counterpart` file.
fn has_sibling_comment(source: &str, start: usize, counterpart: &str) -> bool {
    let comment: Vec<&str> = py_splitlines(&source[..start])
        .into_iter()
        .rev()
        .take_while(|line| line.starts_with("//"))
        .collect();
    let text = comment.join("\n");
    ISSUE_RE.is_match(&text) && text.contains(counterpart)
}

/// Returns one message per finding over `sources`, a list of path and text.
fn findings(sources: &[(&str, String)]) -> Vec<String> {
    let mut found = Vec::new();
    let mut bodies: Vec<(&str, &str)> = Vec::new();
    for (path, source) in sources {
        let Some((start, body)) = find_fn(source, &SIGNATURE) else {
            found.push(format!(
                "{path}: fn {FN_NAME} is missing, not unique or unclosed."
            ));
            continue;
        };
        if UNSUPPORTED.is_match(body) {
            found.push(format!(
                "{path}: fn {FN_NAME} has a block comment or a raw string. \
                 Use line comments and plain strings."
            ));
            continue;
        }
        bodies.push((path, body));
        let counterpart = TARGETS
            .iter()
            .find(|t| *t != path)
            .map_or("", |t| file_name(t));
        if !has_sibling_comment(source, start, counterpart) {
            found.push(format!(
                "{path}: the comment above fn {FN_NAME} must name issue {ISSUE} and {counterpart}."
            ));
        }
    }
    if bodies.windows(2).any(|w| w[0].1 != w[1].1) {
        let names: Vec<&str> = bodies.iter().map(|(p, _)| *p).collect();
        found.push(format!(
            "{} differ. Change both the same way, or merge them (issue {ISSUE}).",
            names.join(" and ")
        ));
    }
    found
}

/// Checks the real files and prints the findings.
fn check() -> Result<ExitCode> {
    let mut sources = Vec::new();
    for rel in TARGETS {
        sources.push((rel, py_read_repo_file(rel)?));
    }
    let found = findings(&sources);
    for message in &found {
        println!("{message}");
    }
    println!("required-migration-versions-sync: {} findings", found.len());
    Ok(crate::util::gate_exit(found.is_empty()))
}

/// Exercises each failure case on fixtures.
fn self_test() -> Result<()> {
    let body = concat!(
        "fn required_migration_versions() -> Result<Vec<String>, String> {\n",
        "    let a = '_';\n",
        "    let b = \"}\"; // }\n",
        "    Ok(vec![])\n",
        "}",
    );
    let doc = "/// Sibling of `other.rs`, `preflight.rs` and `shard_health.rs` (#2358).\n";
    let good = format!("{doc}{body}\n\nfn other() {{}}\n");

    // A brace in a string, a char or a comment does not end the body early.
    let (start, found_body) = find_fn(&good, &SIGNATURE).context("good has no fn")?;
    ensure!(found_body == body, "{found_body:?}");
    ensure!(has_sibling_comment(&good, start, "other.rs"));
    ensure!(!has_sibling_comment(&good, start, "third.rs"));

    // Missing, duplicated and unclosed copies yield no body.
    ensure!(find_fn("fn other() {}\n", &SIGNATURE).is_none());
    ensure!(find_fn(&format!("{body}\n{body}"), &SIGNATURE).is_none());
    let unclosed = &body[..body.rfind('}').context("no brace")?];
    ensure!(find_fn(unclosed, &SIGNATURE).is_none());

    // The sibling comment must name this issue and must touch the fn.
    for text in [
        body.to_string(),
        format!("{doc}\n{body}"),
        format!("/// other.rs #23581.\n{body}"),
        format!("/// other.rs #1.\n{body}"),
    ] {
        let (start, _) = find_fn(&text, &SIGNATURE).context("fixture has no fn")?;
        ensure!(!has_sibling_comment(&text, start, "other.rs"), "{text}");
    }

    let sources = [(TARGETS[0], good.clone()), (TARGETS[1], good.clone())];
    ensure!(findings(&sources).is_empty(), "{:?}", findings(&sources));

    let drifted = [
        (TARGETS[0], good.clone()),
        (TARGETS[1], good.replace("'_'", "'-'")),
    ];
    ensure!(
        findings(&drifted).iter().any(|m| m.contains("differ")),
        "{:?}",
        findings(&drifted)
    );

    let gone = [(TARGETS[0], good.clone()), (TARGETS[1], body.to_string())];
    ensure!(
        findings(&gone).iter().any(|m| m.contains("must name")),
        "{:?}",
        findings(&gone)
    );

    let absent = [
        (TARGETS[0], good.clone()),
        (TARGETS[1], "fn other() {}\n".to_string()),
    ];
    ensure!(
        findings(&absent).iter().any(|m| m.contains("missing")),
        "{:?}",
        findings(&absent)
    );

    // Constructs that the brace matcher cannot read fail closed.
    for risky in ["/* } */", "/* /* } */ */", "let s = r#\"a\"}\"#;"] {
        let text = good.replace("Ok(vec![])", &format!("{risky}\n    Ok(vec![])"));
        let refused = findings(&[(TARGETS[0], text), (TARGETS[1], good.clone())]);
        ensure!(
            refused.iter().any(|m| m.contains("raw string")),
            "{risky}: {refused:?}"
        );
    }
    Ok(())
}

/// Runs `cargo xtask audit required-migration-versions-sync`.
pub fn run(args: Args) -> Result<ExitCode> {
    if !args.self_test {
        return check();
    }
    match self_test() {
        Ok(()) => {
            println!("required-migration-versions-sync self-test: OK");
            Ok(ExitCode::SUCCESS)
        }
        Err(error) => {
            eprintln!("required-migration-versions-sync self-test failed: {error:#}");
            Ok(ExitCode::FAILURE)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_self_test_passes() -> Result<()> {
        self_test()
    }

    #[test]
    fn a_blank_line_breaks_the_sibling_comment() {
        let text = "// names #2358 and a.rs\n\nfn x() {}\n";
        assert!(!has_sibling_comment(text, text.find("fn").unwrap(), "a.rs"));
        let text = "// names #2358 and a.rs\nfn x() {}\n";
        assert!(has_sibling_comment(text, text.find("fn").unwrap(), "a.rs"));
    }
}
