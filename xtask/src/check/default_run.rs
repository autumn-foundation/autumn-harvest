//! `cargo xtask check default-run`: the port of `scripts/check-default-run.sh`.
//!
//! The guard fails if any workspace package defines more than one `[[bin]]`
//! target without a `default-run` in `[package]`.
//!
//! Mechanism this guards against: `cargo run -p <pkg>` (no `--bin`) is the
//! form every README, quickstart and CLI doc uses. It only works when Cargo
//! can pick exactly one binary. A second `[[bin]]` in a package silently
//! breaks every documented `cargo run -p <pkg>` invocation for that package.
//! `cargo build -p <pkg>` still succeeds, because it builds all bin targets.
//! So nothing in a build-only CI job catches it.
//!
//! `examples/quickstart` and `autumn-harvest-cli` both regressed this way.
//! The documented `cargo run -p autumn-harvest-cli -- ...` appears in more
//! than 50 places, including the "Step 3 — Run preflight" step of the
//! quickstart. On trunk-dev it failed with "could not determine which binary
//! to run".
//!
//! This module also holds the helpers that every `check` guard shares, in
//! [`support`].

use std::process::ExitCode;

use serde_json::Value;
use smop::prelude::*;

use crate::util;

#[path = "support.rs"]
pub(super) mod support;

use support::Report;

/// Arguments of `cargo xtask check default-run`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// Runs `cargo xtask check default-run`.
pub fn run(_args: Args) -> Result<ExitCode> {
    let root = util::repo_root();
    let metadata = sh::cargo()
        .args(["metadata", "--no-deps", "--format-version=1"])
        .dir(&root)
        .output_raw()?;
    let parsed = if metadata.status.success() {
        serde_json::from_slice::<Value>(&metadata.stdout).ok()
    } else {
        None
    };
    Ok(evaluate(parsed.as_ref()).emit())
}

/// Lists each package with several bin targets and no `default-run`.
///
/// Each line reads `<name>: bins=['a', 'b']`, the Python list form that the
/// old guard printed. `None` means that the metadata has no package list.
///
/// `None` also means that a field is missing where the old Python parser
/// raised a `KeyError`. Those fields are the `targets` of a package, the
/// `kind` of a target and the `name` of a bin target. The old guard then
/// failed closed. A target that the parser skips silently could hide a
/// second binary.
fn violations(metadata: &Value) -> Option<Vec<String>> {
    let packages = metadata.get("packages")?.as_array()?;
    let mut bad = Vec::new();
    for package in packages {
        let targets = package.get("targets")?.as_array()?;
        let mut bins: Vec<&str> = Vec::new();
        for target in targets {
            let kinds = target.get("kind")?.as_array()?;
            if kinds.iter().any(|kind| kind == "bin") {
                bins.push(target.get("name")?.as_str()?);
            }
        }
        let default_run = package
            .get("default_run")
            .is_some_and(|value| value.as_str().is_some_and(|s| !s.is_empty()));
        if bins.len() > 1 && !default_run {
            let name = package.get("name").and_then(Value::as_str)?;
            bad.push(format!("{name}: bins={}", python_str_list(&bins)));
        }
    }
    Some(bad)
}

/// Renders a list of strings the way Python's `repr` renders a `list[str]`.
fn python_str_list(items: &[&str]) -> String {
    let rendered: Vec<String> = items.iter().map(|item| python_str_repr(item)).collect();
    format!("[{}]", rendered.join(", "))
}

/// Renders one string the way Python's `repr` renders a `str`.
///
/// Python quotes with `'`, unless the text holds a `'` and no `"`.
fn python_str_repr(text: &str) -> String {
    let quote = if text.contains('\'') && !text.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::new();
    out.push(quote);
    for c in text.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            c => out.push(c),
        }
    }
    out.push(quote);
    out
}

/// Builds the guard report from the parsed `cargo metadata` output.
///
/// `None` means that `cargo metadata` failed or printed no JSON.
fn evaluate(metadata: Option<&Value>) -> Report {
    let mut report = Report::new();
    // A failed `cargo metadata` must not print OK over a workspace that the
    // guard never read. So the guard fails closed here.
    let Some(found) = metadata.and_then(violations) else {
        report.fail(
            "cargo xtask check default-run: failed to inspect the workspace (cargo metadata or the parser errored) — failing closed instead of reporting a false OK.",
        );
        return report;
    };
    if !found.is_empty() {
        report.err("The following packages define multiple [[bin]] targets with no default-run,");
        report.err("so `cargo run -p <pkg>` (the form used throughout README.md and docs/) is");
        report.err("ambiguous and fails outright:");
        report.err(found.join("\n"));
        report.err("");
        report.err("Fix: add `default-run = \"<primary-bin-name>\"` under [package] in the");
        report.fail("package's Cargo.toml.");
        return report;
    }
    report.out("OK: every multi-binary package sets default-run.");
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    fn package(name: &str, bins: &[&str], default_run: Option<&str>) -> Value {
        let mut targets: Vec<Value> = bins
            .iter()
            .map(|bin| serde_json::json!({ "name": bin, "kind": ["bin"] }))
            .collect();
        targets.push(serde_json::json!({ "name": name, "kind": ["lib"] }));
        serde_json::json!({ "name": name, "targets": targets, "default_run": default_run })
    }

    #[test]
    fn a_single_binary_and_a_default_run_both_pass() {
        let metadata = serde_json::json!({ "packages": [
            package("one", &["one"], None),
            package("two", &["a", "b"], Some("a")),
        ]});
        let report = evaluate(Some(&metadata));
        assert_eq!(report.code(), 0);
        assert_eq!(
            report.stdout(),
            "OK: every multi-binary package sets default-run.\n"
        );
    }

    #[test]
    fn two_binaries_without_default_run_fail_with_the_python_list() {
        let metadata = serde_json::json!({ "packages": [package("cli", &["cli", "helper"], None)]});
        let report = evaluate(Some(&metadata));
        assert_eq!(report.code(), 1);
        assert!(
            report
                .stderr()
                .contains("\ncli: bins=['cli', 'helper']\n\nFix: add"),
            "{}",
            report.stderr()
        );
    }

    #[test]
    fn missing_metadata_fails_closed() {
        let report = evaluate(None);
        assert_eq!(report.code(), 1);
        assert!(report.stderr().contains("failing closed"));
        let report = evaluate(Some(&serde_json::json!({ "no": "packages" })));
        assert_eq!(report.code(), 1);
    }

    #[test]
    fn a_target_without_a_name_or_kind_fails_closed() {
        let nameless = serde_json::json!({ "packages": [{ "name": "p", "targets": [
            { "kind": ["bin"] },
            { "name": "b", "kind": ["bin"] },
        ]}]});
        let report = evaluate(Some(&nameless));
        assert_eq!(report.code(), 1);
        assert!(report.stderr().contains("failing closed"));
        assert_eq!(report.stdout(), "");
        let kindless = serde_json::json!({ "packages": [{ "name": "p", "targets": [
            { "name": "lib" },
        ]}]});
        assert!(
            evaluate(Some(&kindless))
                .stderr()
                .contains("failing closed")
        );
    }

    #[test]
    fn python_repr_switches_quotes_for_an_apostrophe() {
        assert_eq!(python_str_repr("a"), "'a'");
        assert_eq!(python_str_repr("it's"), "\"it's\"");
        assert_eq!(python_str_repr("a'b\"c"), "'a\\'b\"c'");
    }
}
