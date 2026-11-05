//! `cargo xtask audit audit-catalog-coverage`: the port of
//! `docs/audits/audit-catalog-coverage.py`.
//!
//! This is the Folio corpus harness scan of audit-catalog coverage for
//! `docs/audits/README.md`.
//!
//! The scan is deterministic and reproducible on any checkout. It needs no
//! network access. The file `docs/audits/README.md` is itself a docs page. It
//! is the table that a contributor reads to find out which audit commands
//! exist, what each one checks, and whether CI runs it. `CLAUDE.md` and the
//! docs of several audits point readers at it. Like any other page in the
//! corpus, its table can drift from the thing that it describes. Here the
//! drift is a new audit command whose row nobody adds.
//!
//! This is a coverage check in miniature, over a public surface of one. Each
//! variant of the `Command` enum in `xtask/src/audit/mod.rs` must have a row
//! in `docs/audits/README.md`. The first cell of that row names the command
//! as inline code (`` `cargo xtask audit <name>` ``). This audit is one of
//! those variants, so it must catalog itself. Otherwise a later removal of
//! its own row would go uncaught. A command that exists but has no row is
//! invisible to a reader who only reads the catalog. That is the same failure
//! mode as a coverage-matrix miss anywhere else in the corpus.
//!
//! The reverse also holds. Each row whose first cell names an audit command
//! must name a real one. A row for a removed or renamed command misleads a
//! reader as much as a missing row does.
//!
//! Matching covers the first column of the table only. It is not a substring
//! search over the whole file. The README prose also names commands inline,
//! for example in the comment hygiene section. A command named only in prose,
//! with no row, must still count as undocumented.
//!
//! This audit does not check that the *content* of a row stays accurate,
//! such as what it checks or its CI status. That would need the check text
//! to be a single source of truth. It only checks that a row exists at all.
//! That is the minimum bar for the catalog to be honest about its content.
//!
//! Run `cargo xtask audit audit-catalog-coverage`. The command exits 1 if an
//! audit command has no row, or a row names no command, and 0 otherwise. So
//! it is safe as a CI gate, or as a standalone report.

use std::collections::BTreeSet;
use std::process::ExitCode;
use std::sync::LazyLock;

use regex::Regex;
use smop::prelude::*;

use super::vantage_flash_live_region::{compile, line_of};
use crate::util::read_repo_file;

/// Arguments of `cargo xtask audit audit-catalog-coverage`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

const README: &str = "docs/audits/README.md";

// The first column of the table only: a line that starts with
// `| `cargo xtask audit name` |`. This deliberately does not match a command
// name anywhere else in the file. So a command named only in prose still
// counts as undocumented.
static TABLE_ROW_COMMAND: LazyLock<Regex> =
    LazyLock::new(|| compile(r"(?m)^\|\s*`cargo xtask audit ([\w-]+)`\s*\|"));

/// Returns the name of each `cargo xtask audit` subcommand, sorted.
///
/// The names come from the `Command` enum itself, through clap. So the list
/// cannot drift from the commands that the binary really has.
fn audit_commands() -> Vec<String> {
    let audit = <super::Command as Subcommand>::augment_subcommands(clap::Command::new("audit"));
    let names: BTreeSet<String> = audit
        .get_subcommands()
        .map(|c| c.get_name().to_string())
        .filter(|name| name != "help")
        .collect();
    names.into_iter().collect()
}

/// The two kinds of finding: commands with no row, and rows with no command.
#[derive(Debug, Default, PartialEq, Eq)]
struct Coverage {
    missing: Vec<String>,
    stale: Vec<(usize, String)>,
}

/// Compares the command list against the rows of the README table.
fn coverage(commands: &[String], readme: &str) -> Coverage {
    let rows: Vec<(usize, String)> = TABLE_ROW_COMMAND
        .captures_iter(readme)
        .filter_map(|c| {
            let whole = c.get(0)?;
            Some((line_of(readme, whole.start()), c[1].to_string()))
        })
        .collect();
    let documented: BTreeSet<&str> = rows.iter().map(|(_, name)| name.as_str()).collect();
    let known: BTreeSet<&str> = commands.iter().map(String::as_str).collect();
    Coverage {
        missing: commands
            .iter()
            .filter(|name| !documented.contains(name.as_str()))
            .cloned()
            .collect(),
        stale: rows
            .into_iter()
            .filter(|(_, name)| !known.contains(name.as_str()))
            .collect(),
    }
}

/// Builds the report lines for one comparison.
fn report(commands: &[String], found: &Coverage) -> Vec<String> {
    let mut out = vec![
        format!(
            "Folio audit-catalog coverage — {} audit commands in xtask/src/audit/mod.rs, \
             checked against {README}\n",
            commands.len()
        ),
        format!(
            "Undocumented audit commands (fails CI): {}",
            found.missing.len()
        ),
    ];
    for name in &found.missing {
        out.push(format!(
            "  cargo xtask audit {name} — no `cargo xtask audit {name}` row in {README}"
        ));
    }
    out.push(format!(
        "Rows that name no audit command (fails CI): {}",
        found.stale.len()
    ));
    for (line, name) in &found.stale {
        out.push(format!(
            "  {README}:{line} — `cargo xtask audit {name}` is not a command in xtask/src/audit/mod.rs"
        ));
    }
    out
}

/// Runs `cargo xtask audit audit-catalog-coverage`.
pub fn run(args: Args) -> Result<ExitCode> {
    let Args {} = args;
    let readme = read_repo_file(README)?;
    let commands = audit_commands();
    let found = coverage(&commands, &readme);
    for line in report(&commands, &found) {
        println!("{line}");
    }
    Ok(crate::util::gate_exit(
        found.missing.is_empty() && found.stale.is_empty(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn this_audit_catalogs_itself() {
        let commands = audit_commands();
        assert!(commands.iter().any(|c| c == "audit-catalog-coverage"));
        assert!(commands.iter().any(|c| c == "root-files"));
        assert!(!commands.iter().any(|c| c == "help"));
    }

    #[test]
    fn a_full_table_passes() {
        let readme = "| Command | Checks |\n|---|---|\n| `cargo xtask audit a` | x |\n| `cargo xtask audit b` | y |\n";
        assert_eq!(coverage(&names(&["a", "b"]), readme), Coverage::default());
    }

    #[test]
    fn a_command_named_only_in_prose_is_missing() {
        let readme = "Run `cargo xtask audit b` by hand.\n| `cargo xtask audit a` | x |\n";
        let found = coverage(&names(&["a", "b"]), readme);
        assert_eq!(found.missing, ["b"]);
        let lines = report(&names(&["a", "b"]), &found);
        assert_eq!(
            lines[2],
            "  cargo xtask audit b — no `cargo xtask audit b` row in docs/audits/README.md"
        );
    }

    #[test]
    fn a_row_for_a_removed_command_is_stale() {
        let readme = "| `cargo xtask audit a` | x |\n| `cargo xtask audit gone` | y |\n";
        let found = coverage(&names(&["a"]), readme);
        assert_eq!(found.stale, [(2, "gone".to_string())]);
    }

    #[test]
    fn the_real_catalog_is_complete() -> Result<()> {
        let readme = read_repo_file(README)?;
        assert_eq!(coverage(&audit_commands(), &readme), Coverage::default());
        Ok(())
    }
}
