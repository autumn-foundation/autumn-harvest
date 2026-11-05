//! `cargo xtask check readme-testing-dep-version`: the port of
//! `scripts/check-readme-testing-dep-version.sh`.
//!
//! The guard reads the "CI pattern" replay-safety snippet of `README.md`. It
//! fails if that snippet pins an `autumn-harvest` dev-dependency version that
//! does not match the current minor version of the workspace.
//!
//! Mechanism this guards against: the "Testing workflow code changes with the
//! replayer" section of `README.md` speaks to a reader who just finished the
//! getting-started chapters. Chapter 1 pins `autumn-harvest = "0.6"`
//! correctly. The section tells that reader to add a second, hand-typed
//! version requirement for the same crate as a dev-dependency. Nothing keeps
//! that second pin in sync with the workspace version as releases ship.
//!
//! The semver rule of Cargo for a 0.x requirement is narrow. "0.2" resolves
//! only within 0.2.x. It does not mean "0.2 or later". A live check against
//! the tree reproduced the defect. The workspace version was 0.6.0, but the
//! snippet pinned
//! `autumn-harvest = { version = "0.2", features = ["testing"] }`. That is
//! four minor versions behind the `"0.6"` pin that the reader already has
//! from chapter 1. It also predates features documented two lines below it.
//! Both pins in one Cargo.toml give the same crate two non-overlapping
//! version requirements in `[dependencies]` and `[dev-dependencies]`.

use std::path::Path;
use std::process::ExitCode;

use regex::Regex;
use smop::prelude::*;

use super::default_run::support::{self, Report};
use crate::util;

/// Arguments of `cargo xtask check readme-testing-dep-version`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// Runs `cargo xtask check readme-testing-dep-version`.
pub fn run(_args: Args) -> Result<ExitCode> {
    Ok(evaluate(&util::repo_root())?.emit())
}

/// Returns the workspace version from the root `Cargo.toml`.
///
/// This copies `grep -m1 '^version = "' | sed -E 's/version = "([^"]*)"/\1/'`.
/// It reads the first line that starts with `version = "`, so the result is
/// empty when no such line exists.
pub(super) fn workspace_version(cargo_toml: &str) -> Result<String> {
    let assignment = Regex::new(r#"version = "([^"]*)""#)?;
    Ok(support::lines(cargo_toml)
        .into_iter()
        .find(|line| line.starts_with("version = \""))
        .map(|line| assignment.replace(line, "${1}").into_owned())
        .unwrap_or_default())
}

/// Returns the "### CI pattern" section: the lines after the heading up to the
/// next heading.
///
/// The scope is that section of `README.md`, not the whole file. A correct pin
/// elsewhere in `README.md` must not hide a stale pin in this snippet. A stale
/// pin elsewhere must not fail this guard.
fn ci_pattern_block(readme: &str) -> String {
    let mut inside = false;
    let mut body = Vec::new();
    for line in support::lines(readme) {
        if line == "### CI pattern" {
            inside = true;
            continue;
        }
        if inside && line.starts_with("##") {
            break;
        }
        if inside {
            body.push(line);
        }
    }
    support::join_captured(&body)
}

/// Compares the testing pin of `README.md` with the workspace version.
fn evaluate(root: &Path) -> Result<Report> {
    let mut report = Report::new();
    let full_version = workspace_version(&support::read_or_empty(&root.join("Cargo.toml")))?;
    if full_version.is_empty() {
        report.fail("Could not read the workspace version from Cargo.toml.");
        return Ok(report);
    }
    let minor = Regex::new(r"^([0-9]+\.[0-9]+)\..*")?;
    let minor_version = minor.replace(&full_version, "${1}").into_owned();

    let block = ci_pattern_block(&support::read_or_empty(&root.join("README.md")));
    if block.is_empty() {
        report.fail("Could not find the \"### CI pattern\" section in README.md at all.");
        return Ok(report);
    }

    let pin = Regex::new(r#"autumn-harvest = \{ version = "[^"]*", features = \["testing"\]"#)?;
    let Some(pin_line) = support::lines(&block)
        .into_iter()
        .find(|line| pin.is_match(line))
    else {
        report.err("Could not find the testing-dependency pin (\"autumn-harvest = {");
        report.err("version = ..., features = [\"testing\"] }\") in README.md's CI");
        report.fail("pattern snippet at all.");
        return Ok(report);
    };

    let pinned = Regex::new(r#".*version = "([^"]*)".*"#)?;
    let pinned_version = pinned.replace(pin_line, "${1}").into_owned();

    if pinned_version != minor_version {
        report.err(format!(
            "README.md's CI-pattern snippet pins autumn-harvest = \"{pinned_version}\""
        ));
        report.err("as a dev-dependency, but the current workspace version is");
        report.err(format!("{full_version} (minor {minor_version})."));
        report.err("");
        report.err("Fix: update the pin in README.md to");
        report.fail(format!(
            "  autumn-harvest = {{ version = \"{minor_version}\", features = [\"testing\"] }}"
        ));
        return Ok(report);
    }

    report.out(format!(
        "OK: README.md's testing-dependency pin matches the current workspace version ({minor_version})."
    ));
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(readme: &str) -> smop::fs::TempDir {
        let dir = fs::temp_dir().expect("temp dir");
        std::fs::write(
            dir.path().join("Cargo.toml"),
            "[workspace]\n[workspace.package]\nversion = \"0.9.0\"\n",
        )
        .expect("toml");
        std::fs::write(dir.path().join("README.md"), readme).expect("readme");
        dir
    }

    const PIN: &str = "autumn-harvest = { version = \"0.9\", features = [\"testing\"] }";

    #[test]
    fn a_current_pin_passes() {
        let dir = fixture(&format!(
            "## Testing\n### CI pattern\n```toml\n{PIN}\n```\n## Next\n"
        ));
        let report = evaluate(dir.path()).expect("evaluate");
        assert_eq!(report.code(), 0, "{}", report.stderr());
        assert_eq!(
            report.stdout(),
            "OK: README.md's testing-dependency pin matches the current workspace version (0.9).\n"
        );
    }

    #[test]
    fn a_stale_pin_in_the_section_fails() {
        let stale = PIN.replace("0.9", "0.2");
        let dir = fixture(&format!("### CI pattern\n{stale}\n## Other\n{PIN}\n"));
        let report = evaluate(dir.path()).expect("evaluate");
        assert_eq!(report.code(), 1);
        assert!(report.stderr().starts_with(
            "README.md's CI-pattern snippet pins autumn-harvest = \"0.2\"\nas a dev-dependency, but the current workspace version is\n0.9.0 (minor 0.9).\n"
        ));
    }

    #[test]
    fn a_pin_outside_the_section_does_not_count() {
        let dir = fixture(&format!("### CI pattern\nno pin\n## Other\n{PIN}\n"));
        let report = evaluate(dir.path()).expect("evaluate");
        assert!(
            report
                .stderr()
                .starts_with("Could not find the testing-dependency pin")
        );
        let dir = fixture("no section\n");
        assert!(
            evaluate(dir.path())
                .expect("evaluate")
                .stderr()
                .contains("\"### CI pattern\" section")
        );
    }

    #[test]
    fn workspace_version_reads_the_first_line_only() {
        assert_eq!(
            workspace_version("name = \"x\"\nversion = \"1.2.3\" # pinned\nversion = \"9\"\n")
                .expect("version"),
            "1.2.3 # pinned"
        );
        assert_eq!(workspace_version("[package]\n").expect("version"), "");
    }
}
