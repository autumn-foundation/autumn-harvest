//! `cargo xtask audit ledger-census`: measure the access path of each Ledger-profiled query.
//!
//! This is the port of `docs/audits/ledger-census.py` (issue #2366).
//!
//! `cargo xtask audit ledger-profiles` keeps a profile for every function that
//! sends SQL. Most profiles say `measured = "unmeasured"`. This command
//! measures them in bulk, in two steps.
//!
//! `attribute` reads the raw census that `autumn-harvest/src/ledger_census.rs`
//! writes while the DB suites run. Each line holds one statement and the source
//! frames of its call stack. The command maps the innermost frame that falls in
//! a profiled function to that profile. It writes the result to [`STATEMENTS`],
//! which is committed, so the second step needs no test run.
//!
//! `plan` runs `EXPLAIN (GENERIC_PLAN, FORMAT JSON)` for each statement on a
//! migrated database, with `enable_seqscan = off`. The setting makes the
//! planner take an index whenever one can serve the predicate. A `Seq Scan`
//! that remains means that no index can, whatever the size of the table. So
//! the access path does not depend on fixture data. The step writes one page
//! for each source file under `docs/ledger/plans/`, and it sets the `plan`
//! field of each profile that it measured.
//!
//! `plan-sqlite` does the same for SQLite statements, with `EXPLAIN QUERY PLAN`
//! on an in-memory database built from the SQLite DDL that the census recorded
//! (issue #2429). A `SCAN` that names no index is the finding.
//!
//! A plan is not a buffer count. `measured` stays for a Ledger page with real
//! buffers. A `plan` page shows which index each statement uses, and which
//! statements scan a table with no usable index.
//!
//! CI runs only `--self-test`. The full census needs Postgres, so it runs by hand.

mod attribute;
mod plan;
mod pyjson;
mod selftest;
mod sqlite;

use std::path::Path;
use std::process::ExitCode;

use regex::Regex;
use smop::prelude::*;

use crate::audit::ledger_profiles::scan::Scanner;
use crate::util::repo_root;

/// The committed census statements, relative to the repository root.
pub const STATEMENTS: &str = "docs/perf-artifacts/ledger-census/statements.json";
/// The plan pages, relative to the repository root.
pub const PLANS: &str = "docs/ledger/plans";
/// The access of a table that no index can serve. This is the finding.
pub const NO_INDEX: &str = "no usable index";
/// The access of a full index scan with no index condition.
pub const FULL_INDEX: &str = "full scan of index";
/// The access of a full scan of a partial index.
pub const PARTIAL_INDEX: &str = "full scan of partial index";
/// The marker of a section that the SQLite planner wrote.
pub const SQLITE_SECTION: &str = "Backend: SQLite.";

/// The usage text, printed when the arguments name no step.
///
/// The Python script printed its whole docstring. This is that text, with
/// the command names of the port.
const USAGE: &str = "Measure the access path of each Ledger-profiled query (issue #2366).

`cargo xtask audit ledger-profiles` keeps a profile for every function that
sends SQL. Most profiles say `measured = \"unmeasured\"`. This command measures
them in bulk, in two steps.

`attribute` reads the raw census that `autumn-harvest/src/ledger_census.rs`
writes while the DB suites run. Each line holds one statement and the source
frames of its call stack. The command maps the innermost frame that falls in a
profiled function to that profile. It writes the result to STATEMENTS, which
is committed, so the second step needs no test run.

`plan` runs `EXPLAIN (GENERIC_PLAN, FORMAT JSON)` for each statement on a
migrated database, with `enable_seqscan = off`. The setting makes the planner
take an index whenever one can serve the predicate. A `Seq Scan` that remains
means that no index can, whatever the size of the table. So the access path
does not depend on fixture data. The step writes one page for each source
file under `docs/ledger/plans/`, and it sets the `plan` field of each profile
that it measured.

`plan-sqlite` does the same for SQLite statements, with `EXPLAIN QUERY PLAN`
on an in-memory database built from the SQLite DDL that the census recorded
(issue #2429). A `SCAN` that names no index is the finding.

A plan is not a buffer count. `measured` stays for a Ledger page with real
buffers. A `plan` page shows which index each statement uses, and which
statements scan a table with no usable index.

Usage:
    HARVEST_LEDGER_CENSUS=/var/tmp/census.jsonl \\
        cargo test -p autumn-harvest --test integration -- --test-threads=6
    cargo xtask audit ledger-census attribute /var/tmp/census.jsonl
    cargo xtask audit ledger-census plan postgres://postgres@127.0.0.1/ledger_plan [--driver per_task]
    cargo xtask audit ledger-census plan-sqlite [--driver per_task]
    cargo xtask audit ledger-census --self-test

Leave `HARVEST_TEST_DATABASE_URL` unset, so that each suite boots a migrated
container. The plan database needs every migration in `autumn-harvest/migrations`.
";

/// Arguments of `cargo xtask audit ledger-census`.
#[derive(Debug, Args)]
pub struct Args {
    /// Run the fixtures, then exit.
    #[arg(long)]
    pub self_test: bool,
    /// The step and its arguments: `attribute <raw>`, `plan <url> [--driver D]`
    /// or `plan-sqlite [--driver D]`.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Returns the pattern of a statement that `EXPLAIN` can plan.
///
/// Leading `/* ... */` and `-- ...` comments are skipped, such as the claim
/// ageing marker in `queue.rs`.
pub fn plannable() -> Result<Regex> {
    Regex::new(
        r"(?is)^(?:\s|/\*.*?\*/|--[^\n]*\n)*(?:\(\s*)*(?:SELECT|INSERT|UPDATE|DELETE|WITH|VALUES|MERGE|TABLE)\b",
    )
    .context("compiling the plannable pattern")
}

/// Returns the value after `--driver`, as `argv[argv.index("--driver") + 1]` did.
fn driver_of(argv: &[String]) -> Result<Option<String>> {
    let Some(at) = argv.iter().position(|a| a == "--driver") else {
        return Ok(None);
    };
    argv.get(at + 1)
        .cloned()
        .map(Some)
        .context("`--driver` needs a value")
}

/// Runs `cargo xtask audit ledger-census`.
pub fn run(args: Args) -> Result<ExitCode> {
    let Args {
        self_test,
        rest: steps,
    } = args;
    if self_test || steps.iter().any(|a| a == "--self-test") {
        return Ok(match selftest::self_test() {
            Ok(()) => {
                println!("ledger-census self-test: OK");
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("ledger-census self-test: FAILED: {error:#}");
                ExitCode::FAILURE
            }
        });
    }
    let root = repo_root();
    match steps.first().map(String::as_str) {
        Some("attribute") if steps.len() >= 2 => {
            run_attribute(&root, Path::new(&steps[1]))?;
        }
        Some("plan-sqlite") => {
            sqlite::run_plan_sqlite(&root, driver_of(&steps)?.as_deref())?;
        }
        Some("plan") if steps.len() >= 2 => {
            plan::run_plan(&root, &steps[1], driver_of(&steps)?.as_deref())?;
        }
        _ => {
            println!("{USAGE}");
            return Ok(ExitCode::from(2));
        }
    }
    Ok(ExitCode::SUCCESS)
}

/// Runs the `attribute` step on one raw census file.
fn run_attribute(root: &Path, raw_path: &Path) -> Result<()> {
    let catalogs = crate::audit::ledger_profiles::load_catalogs(root)?;
    let scanner = Scanner::new()?;
    let patterns = attribute::Patterns::new()?;
    let mut resolver = attribute::Resolver::new(root, &catalogs, &scanner);
    let mut skipped = Vec::new();
    let raw = fs::read_string(raw_path)?;
    let new = attribute::attribute(
        raw.split('\n'),
        &mut resolver,
        &patterns,
        Some(&mut skipped),
    )?;
    if !skipped.is_empty() {
        println!("ledger-census: skipped {} torn lines", skipped.len());
    }
    let target = root.join(STATEMENTS);
    let old: attribute::Statements = if target.is_file() {
        serde_json::from_str(&fs::read_string(&target)?).context("parsing the census statements")?
    } else {
        attribute::Statements::new()
    };
    let merged = attribute::merge(&old, &new);
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }
    let value = attribute::statements_value(&merged)?;
    fs::write_string(&target, format!("{}\n", pyjson::dumps_indent(&value, 1)))?;
    let count: usize = merged.values().map(std::collections::BTreeMap::len).sum();
    println!("ledger-census: {count} profiles have statements in {STATEMENTS}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_self_test_fixtures_pass() -> Result<()> {
        selftest::self_test()
    }

    #[test]
    fn driver_needs_a_value() -> Result<()> {
        let argv = |items: &[&str]| items.iter().map(ToString::to_string).collect::<Vec<_>>();
        assert_eq!(
            driver_of(&argv(&["plan-sqlite", "--driver", "per_task"]))?,
            Some("per_task".into())
        );
        assert_eq!(driver_of(&argv(&["plan-sqlite"]))?, None);
        assert!(driver_of(&argv(&["plan-sqlite", "--driver"])).is_err());
        Ok(())
    }

    #[test]
    fn the_committed_statements_round_trip_byte_for_byte() -> Result<()> {
        let text = crate::util::read_repo_file(STATEMENTS)?;
        let statements: attribute::Statements = serde_json::from_str(&text)?;
        let value = attribute::statements_value(&statements)?;
        assert_eq!(format!("{}\n", pyjson::dumps_indent(&value, 1)), text);
        Ok(())
    }
}
