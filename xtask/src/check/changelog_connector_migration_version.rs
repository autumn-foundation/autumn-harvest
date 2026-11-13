//! `cargo xtask check changelog-connector-migration-version`: the port of
//! `scripts/check-changelog-connector-migration-version.sh`.
//!
//! The guard fails if `CHANGELOG.md` cites the wrong migration version for
//! the dead-letter table of the broker connector.
//!
//! Mechanism this guards against: the `[0.6.0]` entry "Add broker
//! event-source connectors" (issue #944) names the poison-message sink as
//! "the plugin-owned `harvest_connector_dead_letters` table (migration
//! `20260719000000`)". That version was wrong. It belongs to a core
//! migration that already shipped,
//! `autumn-harvest/migrations/20260719000000_harvest_workflow_logs` (issue
//! #790). The same `CHANGELOG.md` section cites it correctly two entries
//! below.
//!
//! The real version of the connector migration on disk is `20260719900000`.
//! The `900000` time component is a deliberate bump, per the `up.sql` header
//! of that migration. It stops a collision with a core migration in the one
//! `__diesel_schema_migrations` keyspace of Diesel, which ignores the source.
//! `docs/shipped-work.md` records the collision that this rename fixed.
//!
//! `docs/upgrading/0.6.0.md` sends an upgrading operator to the `[0.6.0]`
//! section of `CHANGELOG.md` to find the migrations of this release. An
//! operator who greps the disk for the cited `20260719000000` finds
//! `harvest_workflow_logs` instead. That operator then concludes that the
//! connector migration is missing, or inspects the wrong table under the
//! right feature name.

use std::path::Path;
use std::process::ExitCode;

use regex::Regex;
use smop::prelude::*;

use super::support::{self, Report};
use crate::util;

const MIGRATIONS: &str = "autumn-harvest-plugin/migrations/harvest";

/// Arguments of `cargo xtask check changelog-connector-migration-version`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// Runs `cargo xtask check changelog-connector-migration-version`.
pub fn run(_args: Args) -> Result<ExitCode> {
    Ok(evaluate(&util::repo_root())?.emit())
}

/// Returns the name of the first `*_harvest_connector_dead_letters` directory.
///
/// The old guard took the first line of `find`, which lists in directory
/// order. The port sorts the names, so the choice does not depend on the
/// file system when two directories match.
fn dead_letter_dir(root: &Path) -> Option<String> {
    let entries = std::fs::read_dir(root.join(MIGRATIONS)).ok()?;
    let mut names: Vec<String> = entries
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| name.ends_with("_harvest_connector_dead_letters"))
        .collect();
    names.sort();
    names.into_iter().next()
}

/// Returns the version prefix of a migration name, like
/// `sed -E 's/^([0-9]+)_.*/\1/'`.
fn version_of(name: &str) -> Result<String> {
    let prefix = Regex::new(r"^([0-9]+)_.*")?;
    Ok(prefix.replace(name, "${1}").into_owned())
}

/// Returns each cited version, one per line.
///
/// This copies the two chained `grep -oE` calls of the old guard.
fn cited_versions(changelog: &str) -> Result<String> {
    let citation = Regex::new(r"harvest_connector_dead_letters. table \(migration .[0-9]+.\)")?;
    let version = Regex::new(r"[0-9]{14}")?;
    let mut found = Vec::new();
    for line in support::lines(changelog) {
        for cite in citation.find_iter(line) {
            for digits in version.find_iter(cite.as_str()) {
                found.push(digits.as_str());
            }
        }
    }
    Ok(support::join_captured(&found))
}

/// Compares the citation in `CHANGELOG.md` with the migration on disk.
fn evaluate(root: &Path) -> Result<Report> {
    let mut report = Report::new();
    let Some(name) = dead_letter_dir(root) else {
        report.err("Could not find a harvest_connector_dead_letters migration directory");
        report.fail("under autumn-harvest-plugin/migrations/harvest/.");
        return Ok(report);
    };
    let actual_dir = format!("{MIGRATIONS}/{name}");
    let actual = version_of(&name)?;

    let cited = cited_versions(&support::read_or_empty(&root.join("CHANGELOG.md")))?;
    if cited.is_empty() {
        report.err("Could not find CHANGELOG.md's migration-version citation for");
        report.fail("harvest_connector_dead_letters at all.");
        return Ok(report);
    }

    if cited != actual {
        report.err(format!("CHANGELOG.md cites migration `{cited}` for"));
        report.err("harvest_connector_dead_letters, but the migration directory on disk");
        report.err(format!("is {actual_dir} (version {actual})."));
        report.err("");
        report.fail(format!(
            "Fix: update CHANGELOG.md's citation to `{actual}`."
        ));
        return Ok(report);
    }

    report.out(format!(
        "OK: CHANGELOG.md cites the correct harvest_connector_dead_letters migration version ({actual})."
    ));
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(dir_name: Option<&str>, changelog: &str) -> smop::fs::TempDir {
        let dir = fs::temp_dir().expect("temp dir");
        let migrations = dir.path().join(MIGRATIONS);
        std::fs::create_dir_all(&migrations).expect("mkdir");
        if let Some(name) = dir_name {
            std::fs::create_dir_all(migrations.join(name)).expect("mkdir");
        }
        std::fs::write(dir.path().join("CHANGELOG.md"), changelog).expect("changelog");
        dir
    }

    const GOOD: &str =
        "- the plugin-owned `harvest_connector_dead_letters` table (migration `20260719900000`).\n";

    #[test]
    fn a_matching_citation_passes() {
        let dir = fixture(Some("20260719900000_harvest_connector_dead_letters"), GOOD);
        let report = evaluate(dir.path()).expect("evaluate");
        assert_eq!(report.code(), 0);
        assert_eq!(
            report.stdout(),
            "OK: CHANGELOG.md cites the correct harvest_connector_dead_letters migration version (20260719900000).\n"
        );
    }

    #[test]
    fn a_wrong_citation_fails() {
        let dir = fixture(
            Some("20260719900000_harvest_connector_dead_letters"),
            &GOOD.replace("20260719900000", "20260719000000"),
        );
        let report = evaluate(dir.path()).expect("evaluate");
        assert_eq!(report.code(), 1);
        assert!(
            report
                .stderr()
                .starts_with("CHANGELOG.md cites migration `20260719000000` for\n")
        );
        assert!(report.stderr().contains(
            "is autumn-harvest-plugin/migrations/harvest/20260719900000_harvest_connector_dead_letters (version 20260719900000)."
        ));
    }

    #[test]
    fn a_missing_directory_or_citation_fails() {
        let dir = fixture(None, GOOD);
        assert!(
            evaluate(dir.path())
                .expect("evaluate")
                .stderr()
                .starts_with("Could not find a harvest_connector_dead_letters")
        );
        let dir = fixture(Some("1_harvest_connector_dead_letters"), "nothing\n");
        assert!(
            evaluate(dir.path())
                .expect("evaluate")
                .stderr()
                .contains("citation for")
        );
    }

    #[test]
    fn two_citations_are_one_mismatch() {
        let doubled = format!("{GOOD}{GOOD}");
        assert_eq!(
            cited_versions(&doubled).expect("cited"),
            "20260719900000\n20260719900000"
        );
    }
}
