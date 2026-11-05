//! `cargo xtask check readme-upgrade-guide-current`: the port of
//! `scripts/check-readme-upgrade-guide-current.sh`.
//!
//! The guard fails if the "Upgrading an existing deployment?" pointer of
//! `README.md` does not link to the upgrade guide for the current workspace
//! version.
//!
//! Mechanism this guards against: `docs/upgrading/` gains a new dated guide
//! each release. The "Where to go next" list of
//! `docs/getting-started/README.md` names the new one. But the top-level
//! "Upgrading an existing deployment?" pointer of `README.md` is a separate,
//! hand-written link, and nothing keeps it in sync with either.
//!
//! A live check against the tree reproduced the defect. The workspace
//! version was 0.6.0, and `docs/upgrading/0.6.0.md` existed.
//! `docs/getting-started/README.md` already called it "the current" guide.
//! Yet `README.md` still pointed at `docs/upgrading/0.5.0.md`, a superseded
//! 0.4.0 → 0.5.0 guide. A reader who follows the front door of the repository
//! to upgrade a deployment gets the wrong guide. That reader can miss the
//! defining change of the release entirely: the move of migration ownership
//! to Autumn in 0.6.0. The defect is more than cosmetic staleness.

use std::path::Path;
use std::process::ExitCode;

use regex::Regex;
use smop::prelude::*;

use super::default_run::support::{self, Report};
use super::readme_testing_dep_version::workspace_version;
use crate::util;

/// Arguments of `cargo xtask check readme-upgrade-guide-current`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// Runs `cargo xtask check readme-upgrade-guide-current`.
pub fn run(_args: Args) -> Result<ExitCode> {
    Ok(evaluate(&util::repo_root())?.emit())
}

/// Returns the pointer paragraph: the anchor line through the next blank line.
///
/// The scope is the paragraph of the pointer, not the whole file. A correct
/// link can appear elsewhere in `README.md`, in a historical aside or a
/// release note. That link must not satisfy this guard while the pointer
/// itself stays stale.
fn pointer_block(readme: &str) -> String {
    let mut inside = false;
    let mut body = Vec::new();
    for line in support::lines(readme) {
        if line.contains("Upgrading an existing deployment?") {
            inside = true;
        }
        if inside {
            body.push(line);
            if line.is_empty() {
                break;
            }
        }
    }
    support::join_captured(&body)
}

/// Checks the pointer of `README.md` under `root`.
fn evaluate(root: &Path) -> Result<Report> {
    let mut report = Report::new();
    let version = workspace_version(&support::read_or_empty(&root.join("Cargo.toml")))?;
    let guide = format!("docs/upgrading/{version}.md");

    if version.is_empty() {
        report.fail("Could not read the workspace version from Cargo.toml.");
        return Ok(report);
    }

    if !root.join(&guide).is_file() {
        report.err(format!(
            "Workspace version is {version}, but {guide} does not exist."
        ));
        report.err("If this release genuinely needs no upgrade guide, update this");
        report.fail("script's expectations instead of leaving it red.");
        return Ok(report);
    }

    let block = pointer_block(&support::read_or_empty(&root.join("README.md")));
    if block.is_empty() {
        report.err("Could not find the \"Upgrading an existing deployment?\" pointer in");
        report.fail("README.md at all.");
        return Ok(report);
    }

    // The paragraph can name more than one guide: the current one, and the
    // hop before it for readers further behind. Only the FIRST markdown link
    // is the primary pointer that readers follow, so the guard checks that
    // one. A correct link later in the paragraph must not hide a stale
    // primary link.
    // `grep -o` matches within one line, so a link never spans two lines.
    let link = Regex::new(r"\]\(([^)]+)\)")?;
    let primary = support::lines(&block)
        .into_iter()
        .find_map(|line| link.captures(line))
        .and_then(|caps| caps.get(1))
        .map(|found| found.as_str().to_owned())
        .unwrap_or_default();

    if primary != guide {
        report.err("README.md's \"Upgrading an existing deployment?\" pointer's primary");
        report.err(format!(
            "link does not point to {guide}, the guide for the current workspace"
        ));
        report.err(format!("version ({version})."));
        let shown = if primary.is_empty() {
            "<no markdown link found in that paragraph>"
        } else {
            &primary
        };
        report.err(format!("Its primary link currently points to: {shown}"));
        report.err("");
        report.fail(format!(
            "Fix: make the first link in that paragraph point at {guide}."
        ));
        return Ok(report);
    }

    report.out(format!(
        "OK: README.md's upgrade pointer links to the current guide ({guide})."
    ));
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(readme: &str, guide: bool) -> smop::fs::TempDir {
        let dir = fs::temp_dir().expect("temp dir");
        std::fs::write(dir.path().join("Cargo.toml"), "version = \"0.9.0\"\n").expect("toml");
        std::fs::write(dir.path().join("README.md"), readme).expect("readme");
        if guide {
            std::fs::create_dir_all(dir.path().join("docs/upgrading")).expect("mkdir");
            std::fs::write(dir.path().join("docs/upgrading/0.9.0.md"), "# 0.9.0\n").expect("guide");
        }
        dir
    }

    #[test]
    fn a_current_primary_link_passes() {
        let dir = fixture(
            "**Upgrading an existing deployment?** Read\n[the guide](docs/upgrading/0.9.0.md), then [the hop](docs/upgrading/0.8.0.md).\n\n[x](docs/upgrading/0.1.0.md)\n",
            true,
        );
        let report = evaluate(dir.path()).expect("evaluate");
        assert_eq!(report.code(), 0, "{}", report.stderr());
    }

    #[test]
    fn a_stale_primary_link_fails_even_with_a_current_second_link() {
        let dir = fixture(
            "Upgrading an existing deployment? See [old](docs/upgrading/0.5.0.md) or [new](docs/upgrading/0.9.0.md).\n",
            true,
        );
        let report = evaluate(dir.path()).expect("evaluate");
        assert_eq!(report.code(), 1);
        assert!(
            report
                .stderr()
                .contains("Its primary link currently points to: docs/upgrading/0.5.0.md\n")
        );
    }

    #[test]
    fn a_paragraph_without_links_and_a_missing_guide_fail() {
        let dir = fixture(
            "Upgrading an existing deployment? Ask.\n\n[late](docs/upgrading/0.9.0.md)\n",
            true,
        );
        let report = evaluate(dir.path()).expect("evaluate");
        assert!(
            report
                .stderr()
                .contains("points to: <no markdown link found in that paragraph>\n")
        );
        let dir = fixture("x\n", false);
        let report = evaluate(dir.path()).expect("evaluate");
        assert!(report.stderr().starts_with(
            "Workspace version is 0.9.0, but docs/upgrading/0.9.0.md does not exist.\n"
        ));
    }
}
