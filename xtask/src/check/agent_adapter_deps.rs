//! `cargo xtask check agent-adapter-deps`: the port of
//! `scripts/check-agent-adapter-deps.sh`.
//!
//! The guard fails if `autumn-harvest-agent` depends on an Autumn crate
//! outside the engine.
//!
//! Issue #1973 requires the agent adapter to work on the core crate with no
//! `autumn-web`. The adapter also owns its agent primitives. So it must not
//! pull in an Autumn plugin either (`autumn-plugin-agent`,
//! `autumn-harvest-plugin`). One changed dependency line would bring the web
//! stack back, and no compile error would show it. This guard reads the
//! resolved graph instead.

use std::process::ExitCode;

use regex::Regex;
use smop::prelude::*;

use super::support::Report;
use crate::util;

/// Arguments of `cargo xtask check agent-adapter-deps`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// Runs `cargo xtask check agent-adapter-deps`.
pub fn run(_args: Args) -> Result<ExitCode> {
    let tree = sh::cargo()
        .args([
            "tree",
            "--locked",
            "-p",
            "autumn-harvest-agent",
            "--all-features",
            "-e",
            "normal",
            "--prefix",
            "none",
        ])
        .dir(util::repo_root())
        .output_raw()?;
    // The old guard let cargo write its stderr straight through.
    eprint!("{}", String::from_utf8_lossy(&tree.stderr));
    if !tree.status.success() {
        // Under `set -e` a failed `cargo tree` ended the guard with its code.
        let code = tree.status.code().unwrap_or(1);
        return Ok(ExitCode::from(u8::try_from(code).unwrap_or(1)));
    }
    let graph = String::from_utf8_lossy(&tree.stdout);
    Ok(evaluate(&graph)?.emit())
}

/// Checks the `cargo tree --prefix none` output of the agent crate.
fn evaluate(graph: &str) -> Result<Report> {
    let engine = Regex::new(r"^autumn-harvest(-agent|-macros|-sqlite)? ")?;
    let mut foreign: Vec<&str> = graph
        .lines()
        .filter(|line| line.starts_with("autumn-") && !engine.is_match(line))
        .collect();
    foreign.sort_unstable();
    foreign.dedup();

    let mut report = Report::new();
    if !foreign.is_empty() {
        report.err("FAIL: autumn-harvest-agent depends on Autumn crates outside the engine:");
        report.fail(foreign.join("\n"));
        return Ok(report);
    }
    if !graph
        .lines()
        .any(|line| line.starts_with("autumn-harvest "))
    {
        report.fail(
            "FAIL: autumn-harvest is missing from the graph; the check reads the wrong crate.",
        );
        return Ok(report);
    }
    report.out("OK: autumn-harvest-agent depends on no Autumn crate outside the engine.");
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_engine_only_graph_passes() {
        let graph = "autumn-harvest-agent v0.9.0 (/r)\nautumn-harvest v0.9.0 (/r)\nautumn-harvest-macros v0.9.0 (proc-macro) (/r)\nserde v1.0.0\n";
        let report = evaluate(graph).expect("evaluate");
        assert_eq!(report.code(), 0);
        assert!(report.stdout().starts_with("OK:"));
    }

    #[test]
    fn a_plugin_dependency_fails_once_per_crate() {
        let graph = "autumn-harvest v0.9.0 (/r)\nautumn-web v1.0.0\nautumn-harvest-plugin v0.9.0 (/r)\nautumn-web v1.0.0\n";
        let report = evaluate(graph).expect("evaluate");
        assert_eq!(report.code(), 1);
        assert_eq!(
            report.stderr(),
            "FAIL: autumn-harvest-agent depends on Autumn crates outside the engine:\nautumn-harvest-plugin v0.9.0 (/r)\nautumn-web v1.0.0\n"
        );
    }

    #[test]
    fn a_graph_without_the_engine_fails() {
        let report = evaluate("serde v1.0.0\n").expect("evaluate");
        assert_eq!(report.code(), 1);
        assert!(report.stderr().contains("missing from the graph"));
    }
}
