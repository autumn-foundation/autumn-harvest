//! `cargo xtask audit mixed-version-contract`: the mixed-version contract
//! guard (issue #1828).
//!
//! This is the port of `docs/audits/mixed-version-contract.py`.
//!
//! The rolling-deploy contract has two halves. `docs/upgrading/README.md`
//! states it. The `mixed-version-smoke` CI job proves it. This guard fails
//! when either half goes missing, or when the two stop naming each other.
//!
//! Checks:
//!
//! 1. `docs/upgrading/README.md` has each contract section.
//! 2. The page names the smoke command and the CI job (as `` `job` ``).
//! 3. `ci.yml` has a `mixed-version-smoke` job that runs the smoke command.
//! 4. The xtask module of the smoke command exists. The Python script checked
//!    that the shell script was executable. The smoke now runs as
//!    `cargo xtask dev run-mixed-version-smoke`, so its module takes that place.
//! 5. The smoke crate declares its own `[workspace]`. Without it, Cargo rejects
//!    the crate as an unlisted member of the root workspace.
//!
//! `--self-test` runs the checks against fixtures, so a broken check cannot
//! pass silently.
//!
//! Usage: `cargo xtask audit mixed-version-contract [--self-test]`.

use std::path::Path;
use std::process::ExitCode;
use std::sync::LazyLock;

use regex::Regex;
use smop::prelude::*;

use super::workflow_yaml_parse::pytext::{self, repr_str};
use crate::util;

/// Arguments of `cargo xtask audit mixed-version-contract`.
#[derive(Debug, Args)]
pub struct Args {
    /// Run the checks against fixtures instead of the repository.
    #[arg(long)]
    pub self_test: bool,
}

const DOC: &str = "docs/upgrading/README.md";
const CI: &str = ".github/workflows/ci.yml";
const SMOKE: &str = "cargo xtask dev run-mixed-version-smoke";
const MODULE: &str = "xtask/src/dev/run_mixed_version_smoke.rs";
/// The directory form of the same module, when it grows submodules.
const MODULE_DIR: &str = "xtask/src/dev/run_mixed_version_smoke/mod.rs";
const CRATE: &str = "scripts/mixed-version-smoke/Cargo.toml";
const JOB: &str = "mixed-version-smoke";

/// Each heading holds one part of the contract. The doc may add others.
const SECTIONS: [&str; 5] = [
    "## Supported version skew",
    "## Schema changes: expand, then contract",
    "## Event and codec formats",
    "## Rolling deploy order",
    "## How CI proves the contract",
];

static JOB_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(
        r"(?m)^  {}:\n((?:(?:    .*)?\n)*)",
        regex::escape(JOB)
    ))
    .unwrap_or_else(|error| unreachable!("job pattern: {error}"))
});
static WORKSPACE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?m)^\[workspace\]\s*$")
        .unwrap_or_else(|error| unreachable!("workspace pattern: {error}"))
});

/// Returns the findings for the contract page.
fn check_doc(text: Option<&str>) -> Vec<String> {
    let Some(text) = text else {
        return vec![format!("{DOC}: missing")];
    };
    let mut found = Vec::new();
    let lines: Vec<&str> = pytext::splitlines(text)
        .into_iter()
        .map(pytext::rstrip)
        .collect();
    for heading in SECTIONS {
        if !lines.contains(&heading) {
            found.push(format!("{DOC}: no `{heading}` heading"));
        }
    }
    // The job name is part of the smoke command, so match it as a code span.
    for name in [SMOKE.to_owned(), format!("`{JOB}`")] {
        if !text.contains(&name) {
            found.push(format!("{DOC}: does not name {name}"));
        }
    }
    found
}

/// Returns the text of the smoke job, or `None`.
fn job_block(ci_text: &str) -> Option<&str> {
    JOB_RE
        .captures(ci_text)
        .and_then(|found| found.get(1))
        .map(|body| body.as_str())
}

/// Returns the findings for the CI workflow.
fn check_ci(text: Option<&str>) -> Vec<String> {
    let Some(text) = text else {
        return vec![format!("{CI}: missing")];
    };
    let Some(block) = job_block(text) else {
        return vec![format!("{CI}: no `{JOB}` job")];
    };
    if !block.contains(SMOKE) {
        return vec![format!("{CI}: the `{JOB}` job does not run `{SMOKE}`")];
    }
    Vec::new()
}

/// Returns the findings for the xtask module of the smoke command.
fn check_module(exists: bool) -> Vec<String> {
    if exists {
        Vec::new()
    } else {
        vec![format!("{MODULE}: missing")]
    }
}

/// Returns the findings for the smoke crate manifest.
fn check_crate(text: Option<&str>) -> Vec<String> {
    let Some(text) = text else {
        return vec![format!("{CRATE}: missing")];
    };
    if !WORKSPACE_RE.is_match(text) {
        return vec![format!("{CRATE}: no `[workspace]` table")];
    }
    Vec::new()
}

/// Runs every check under `root`. Returns the findings.
fn audit(root: &Path) -> Result<Vec<String>> {
    let read = |rel: &str| pytext::read_text(&root.join(rel));
    let module = root.join(MODULE).is_file() || root.join(MODULE_DIR).is_file();
    let mut findings = check_doc(read(DOC)?.as_deref());
    findings.extend(check_ci(read(CI)?.as_deref()));
    findings.extend(check_module(module));
    findings.extend(check_crate(read(CRATE)?.as_deref()));
    Ok(findings)
}

/// Returns (findings, expected count) for each self-test case.
fn self_test_cases() -> Vec<(Vec<String>, usize)> {
    let good_doc = format!("{}\nRun `{SMOKE}` in job `{JOB}`.\n", SECTIONS.join("\n"));
    let good_ci = format!("jobs:\n  {JOB}:\n    steps:\n      - run: {SMOKE}\n  other:\n");
    vec![
        (check_doc(Some(&good_doc)), 0),
        (check_doc(None), 1),
        (
            check_doc(Some(&good_doc.replace(SECTIONS[2], "## Formats"))),
            1,
        ),
        (check_doc(Some(&good_doc.replace(SMOKE, "x.sh"))), 1),
        (
            check_doc(Some(&good_doc.replace(&format!("`{JOB}`"), "`x`"))),
            1,
        ),
        (check_ci(Some(&good_ci)), 0),
        (check_ci(None), 1),
        (check_ci(Some("jobs:\n  lint:\n    steps: []\n")), 1),
        // The smoke command named in another job does not count.
        (
            check_ci(Some(&format!(
                "jobs:\n  {JOB}:\n    steps: []\n  b:\n    run: {SMOKE}\n"
            ))),
            1,
        ),
        (check_module(true), 0),
        (check_module(false), 1),
        (check_crate(Some("[package]\n\n[workspace]\n")), 0),
        (check_crate(Some("[package]\n")), 1),
        (check_crate(None), 1),
    ]
}

/// Renders findings as Python `repr` of a list of `str`.
fn repr_list(items: &[String]) -> String {
    let items: Vec<String> = items.iter().map(|item| repr_str(item)).collect();
    format!("[{}]", items.join(", "))
}

/// Runs the fixtures. Returns the output lines and the count of failed cases.
fn self_test() -> (Vec<String>, usize) {
    let cases = self_test_cases();
    let mut lines = Vec::new();
    let mut bad = 0;
    for (index, (got, want)) in cases.iter().enumerate() {
        if got.len() != *want {
            lines.push(format!("self-test case {index}: got {}", repr_list(got)));
            bad += 1;
        }
    }
    lines.push(format!(
        "mixed-version-contract self-test: {} cases, {bad} failed",
        cases.len()
    ));
    (lines, bad)
}

/// Runs `cargo xtask audit mixed-version-contract`.
pub fn run(args: Args) -> Result<ExitCode> {
    let (lines, bad) = if args.self_test {
        self_test()
    } else {
        let findings = audit(&util::repo_root())?;
        let count = findings.len();
        let mut lines = findings;
        lines.push(format!("mixed-version-contract: {count} findings"));
        (lines, count)
    };
    for line in lines {
        println!("{line}");
    }
    Ok(util::gate_exit(bad == 0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_self_test_case_passes() {
        let (lines, bad) = self_test();
        assert_eq!(bad, 0, "{lines:#?}");
        assert_eq!(
            lines,
            ["mixed-version-contract self-test: 14 cases, 0 failed"]
        );
    }

    #[test]
    fn the_job_block_stops_at_the_next_job() {
        let ci = "jobs:\n  mixed-version-smoke:\n    a: 1\n\n    b: 2\n  next:\n    run: x\n";
        assert_eq!(job_block(ci), Some("    a: 1\n\n    b: 2\n"));
        assert_eq!(job_block("jobs:\n  other:\n"), None);
    }

    #[test]
    fn a_heading_with_trailing_space_still_counts() {
        let doc = format!("{} \t\n{SMOKE} `{JOB}`\n", SECTIONS.join("  \n"));
        assert_eq!(check_doc(Some(&doc)), Vec::<String>::new());
    }

    #[test]
    fn a_failed_case_reports_its_findings_as_a_python_list() {
        assert_eq!(repr_list(&["a: missing".to_owned()]), "['a: missing']");
    }

    #[test]
    fn a_missing_tree_reports_each_part() {
        let dir = fs::temp_dir().unwrap_or_else(|error| panic!("{error:#}"));
        let findings = audit(dir.path()).unwrap_or_default();
        assert_eq!(
            findings,
            [
                format!("{DOC}: missing"),
                format!("{CI}: missing"),
                format!("{MODULE}: missing"),
                format!("{CRATE}: missing"),
            ]
        );
    }
}
