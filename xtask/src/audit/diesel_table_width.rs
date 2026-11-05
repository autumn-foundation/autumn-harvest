//! `cargo xtask audit diesel-table-width`: the port of
//! `docs/audits/diesel-table-width.py`.
//!
//! It keeps every diesel `table!` narrow (issue #2492).
//!
//! Diesel implements its traits for tuples up to a fixed size. A cargo
//! feature sets the size: 32 by default, or `64-column-tables`, or
//! `128-column-tables`. A wider tier generates impls for every tuple size up
//! to its limit. That costs compile time and memory in every crate that
//! builds diesel. Cargo unifies features, so one wide table in this
//! workspace makes every embedder pay for the wide tier as well.
//!
//! The command fails in five cases:
//! - A `table!` has more than 64 columns.
//! - A `table!` has more than 32 columns and is not in `WIDE_TABLES`.
//! - A table in `WIDE_TABLES` is wider than its ceiling.
//! - A table in `WIDE_TABLES` is narrower than its ceiling. Lower the
//!   ceiling, or remove the entry at 32 columns or fewer. That makes the list
//!   a ratchet.
//! - A manifest enables `128-column-tables`. When `WIDE_TABLES` is empty, a
//!   manifest that enables `64-column-tables` fails too.
//!
//! Do not add a ceiling. Split a wide table over a core and an extension
//! declaration instead. `autumn-harvest/src/table_split.rs` shows how.
//!
//! Usage: `cargo xtask audit diesel-table-width [--self-test]`.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Component, Path, PathBuf};
use std::process::ExitCode;
use std::sync::LazyLock;

use regex::Regex;
use smop::prelude::*;
use walkdir::WalkDir;

use super::vantage_flash_live_region::compile;
use crate::util::repo_root;

/// Arguments of `cargo xtask audit diesel-table-width`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {
    /// Run the fixtures instead of the real tree.
    #[arg(long)]
    pub self_test: bool,
}

const HARD_LIMIT: usize = 64;
const TARGET: usize = 32;
const SKIP_DIRS: &[&str] = &["target", ".git", "node_modules"];

// Tables over `TARGET` columns, by name, with their column count. The list
// is empty: every table fits the default tier (issue #2492). A table wider
// than `TARGET` splits over two declarations instead, through
// `autumn-harvest/src/table_split.rs`. Do not add an entry.
const WIDE_TABLES: &[(&str, usize)] = &[];

// Manifests that the workspace never builds. The assay apparatus crates pin
// the dependency set of a recorded run, so they keep their own features.
const MANIFEST_SKIP_DIRS: &[&str] = &["docs"];

static TABLE_OPEN: LazyLock<Regex> = LazyLock::new(|| compile(r"\btable!\s*\{"));
// The table name and its primary key, then the opening brace of the body.
static TABLE_HEAD: LazyLock<Regex> = LazyLock::new(|| compile(r"([\w.]+)\s*(?:\([^)]*\))?\s*\{"));
// One column: optional attributes, a name, then `->`.
static COLUMN: LazyLock<Regex> =
    LazyLock::new(|| compile(r"(?m)^\s*(?:#\[[^\]]*\]\s*)*(?:r#)?\w+\s*->"));
static DOC_LINE: LazyLock<Regex> = LazyLock::new(|| compile(r"(?m)^\s*//[^\n]*$"));
static WIDE_FEATURE: LazyLock<Regex> = LazyLock::new(|| compile(r#""(64|128)-column-tables""#));

/// Returns the name, column count and line of each `table!` in `source`.
fn tables_in(source: &str) -> Vec<(String, usize, usize)> {
    let bytes = source.as_bytes();
    let mut out = Vec::new();
    for open in TABLE_OPEN.find_iter(source) {
        let mut depth = 1;
        let mut pos = open.end();
        while depth > 0 && pos < bytes.len() {
            match bytes[pos] {
                b'{' => depth += 1,
                b'}' => depth -= 1,
                _ => {}
            }
            pos += 1;
        }
        // The Python slice `source[end:pos - 1]` drops the last character.
        // A closed body ends on the ASCII `}`. An unclosed body drops the
        // last character of the file, which can be wider than one byte.
        let body_end = if depth == 0 {
            pos - 1
        } else {
            source.char_indices().next_back().map_or(0, |(i, _)| i)
        }
        .max(open.end());
        let body = DOC_LINE.replace_all(&source[open.end()..body_end], "");
        let Some(head) = TABLE_HEAD.captures(&body) else {
            continue;
        };
        let (Some(whole), Some(name)) = (head.get(0), head.get(1)) else {
            continue;
        };
        let columns = COLUMN.find_iter(&body[whole.end()..]).count();
        let line = source[..open.start()].matches('\n').count() + 1;
        let name = name.as_str().rsplit('.').next().unwrap_or_default();
        out.push((name.to_string(), columns, line));
    }
    out
}

/// Returns each file under `root` whose name ends with `suffix`.
///
/// The order is the order of Python's `sorted(root.rglob(...))`: path
/// components compare one by one. A path with a component in `SKIP_DIRS` is
/// left out. Symbolic links to directories are not followed, as in
/// Python 3.13 `rglob`.
fn walk(root: &Path, suffix: &str) -> Vec<PathBuf> {
    let mut found: Vec<PathBuf> = WalkDir::new(root)
        .min_depth(1)
        .into_iter()
        .filter_map(std::result::Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(suffix))
        .filter_map(|entry| entry.path().strip_prefix(root).ok().map(Path::to_path_buf))
        .filter(|rel| {
            !rel.components().any(|c| match c {
                Component::Normal(part) => SKIP_DIRS.iter().any(|skip| part == *skip),
                _ => false,
            })
        })
        .collect();
    found.sort_by(|a, b| a.components().cmp(b.components()));
    found
}

/// Returns a failure for each `(name, count, where)` entry that breaks a rule.
fn check_tables(
    found: &[(String, usize, String)],
    wide_tables: &BTreeMap<String, usize>,
) -> Vec<String> {
    let mut failures = Vec::new();
    let mut widest: BTreeMap<&str, usize> = BTreeMap::new();
    for (name, count, place) in found {
        let ceiling = wide_tables.get(name);
        if *count > HARD_LIMIT {
            failures.push(format!(
                "{place}: `{name}` has {count} columns. The limit is {HARD_LIMIT}. \
                 Move columns out of the `table!`."
            ));
        } else if *count > TARGET && ceiling.is_none() {
            failures.push(format!(
                "{place}: `{name}` has {count} columns. A new table has {TARGET} or fewer. \
                 Split it, or move columns out."
            ));
        } else if let Some(ceiling) = ceiling.filter(|c| count > c) {
            failures.push(format!(
                "{place}: `{name}` has {count} columns, over its ceiling of {ceiling}. \
                 Move a column out instead."
            ));
        }
        let entry = widest.entry(name.as_str()).or_insert(0);
        *entry = (*entry).max(*count);
    }
    for (name, ceiling) in wide_tables {
        match widest.get(name.as_str()) {
            None => failures.push(format!("WIDE_TABLES: `{name}` has no `table!`. Remove it.")),
            Some(&count) if count <= TARGET => failures.push(format!(
                "WIDE_TABLES: `{name}` has {count} columns. Remove its entry."
            )),
            Some(&count) if count < *ceiling => failures.push(format!(
                "WIDE_TABLES: `{name}` has {count} columns. Lower its ceiling from {ceiling} to {count}."
            )),
            Some(_) => {}
        }
    }
    failures
}

/// Returns a failure for each wide feature that one `Cargo.toml` enables.
fn check_manifest(text: &str, place: &str, wide_tables: &BTreeMap<String, usize>) -> Vec<String> {
    WIDE_FEATURE
        .captures_iter(text)
        .filter_map(|m| {
            let tier = &m[1];
            (tier == "128" || wide_tables.is_empty()).then(|| {
                format!(
                    "{place}: enables `{tier}-column-tables`. Every table fits \
                     a narrower tier, so remove the feature."
                )
            })
        })
        .collect()
}

/// Returns `WIDE_TABLES` as a map.
fn wide_tables() -> BTreeMap<String, usize> {
    WIDE_TABLES
        .iter()
        .map(|(name, ceiling)| ((*name).to_string(), *ceiling))
        .collect()
}

/// Checks the tree at `root` and returns the list of failures.
fn check_tree(root: &Path) -> Result<Vec<String>> {
    let wide = wide_tables();
    let mut found = Vec::new();
    for rel in walk(root, ".rs") {
        let bytes =
            std::fs::read(root.join(&rel)).with_context(|| format!("reading {}", rel.display()))?;
        let source = String::from_utf8_lossy(&bytes);
        for (name, count, line) in tables_in(&source) {
            found.push((name, count, format!("{}:{line}", rel.display())));
        }
    }
    let mut failures = check_tables(&found, &wide);
    for rel in walk(root, "Cargo.toml") {
        let top = rel.components().next();
        let skipped = matches!(top, Some(Component::Normal(part)) if MANIFEST_SKIP_DIRS.iter().any(|d| part == *d));
        if skipped {
            continue;
        }
        let text = fs::read_string(root.join(&rel))?;
        failures.extend(check_manifest(&text, &rel.display().to_string(), &wide));
    }
    Ok(failures)
}

/// Returns a `table!` fixture with `columns` columns.
fn table_source(name: &str, columns: usize) -> String {
    let body = (0..columns).fold(String::new(), |mut acc, i| {
        let _ = write!(acc, "        /// Doc.\n        c{i} -> Int4,\n");
        acc
    });
    format!(
        "diesel::{}! {{\n    {name} (c0) {{\n{body}    }}\n}}\n",
        "table"
    )
}

/// Exercises each failure case on fixtures.
fn self_test() -> Result<()> {
    let count = |source: &str| -> Vec<(String, usize)> {
        tables_in(source)
            .into_iter()
            .map(|(n, c, _)| (n, c))
            .collect()
    };
    ensure!(count(&table_source("t", 3)) == [("t".to_string(), 3)]);
    let attr = format!(
        "{}! {{ s.t (id) {{\n #[sql_name = \"x\"]\n id -> Int4,\n r#type -> Text,\n}} }}",
        "table"
    );
    ensure!(count(&attr) == [("t".to_string(), 2)], "{:?}", count(&attr));

    let wide = |entries: &[(&str, usize)]| -> BTreeMap<String, usize> {
        entries
            .iter()
            .map(|(n, c)| ((*n).to_string(), *c))
            .collect()
    };
    let fails = |name: &str, columns: usize, entries: &[(&str, usize)]| {
        !check_tables(
            &[(name.to_string(), columns, "f.rs:1".to_string())],
            &wide(entries),
        )
        .is_empty()
    };
    ensure!(!fails("t", TARGET, &[]));
    ensure!(fails("t", TARGET + 1, &[]));
    ensure!(fails("t", HARD_LIMIT + 1, &[("t", HARD_LIMIT + 1)]));
    ensure!(!fails("t", 40, &[("t", 40)]));
    ensure!(fails("t", 41, &[("t", 40)]));
    ensure!(fails("t", 39, &[("t", 40)]));
    ensure!(fails("t", TARGET, &[("t", 40)]));
    ensure!(!check_tables(&[], &wide(&[("t", 40)])).is_empty());

    let feature = "diesel = { features = [\"64-column-tables\"] }";
    ensure!(check_manifest(feature, "Cargo.toml", &wide(&[("t", 40)])).is_empty());
    ensure!(!check_manifest(feature, "Cargo.toml", &wide(&[])).is_empty());
    ensure!(
        !check_manifest(
            &feature.replace("64", "128"),
            "Cargo.toml",
            &wide(&[("t", 40)])
        )
        .is_empty()
    );
    Ok(())
}

/// Runs `cargo xtask audit diesel-table-width`.
pub fn run(args: Args) -> Result<ExitCode> {
    if args.self_test {
        return match self_test() {
            Ok(()) => {
                println!("diesel-table-width self-test passed");
                Ok(ExitCode::SUCCESS)
            }
            Err(error) => {
                eprintln!("diesel-table-width self-test failed: {error:#}");
                Ok(ExitCode::FAILURE)
            }
        };
    }
    let failures = check_tree(&repo_root())?;
    for failure in &failures {
        println!("{failure}");
    }
    if failures.is_empty() {
        println!("diesel-table-width: every table! fits its ceiling");
        return Ok(ExitCode::SUCCESS);
    }
    println!("diesel-table-width: {} failure(s)", failures.len());
    Ok(ExitCode::FAILURE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_self_test_passes() -> Result<()> {
        self_test()
    }

    #[test]
    fn a_wide_table_in_a_tree_fails() -> Result<()> {
        let dir = fs::temp_dir()?;
        let root = dir.path();
        std::fs::create_dir_all(root.join("src"))?;
        std::fs::create_dir_all(root.join("target"))?;
        std::fs::create_dir_all(root.join("docs/x"))?;
        fs::write_string(root.join("src/schema.rs"), table_source("wide", TARGET + 1))?;
        fs::write_string(root.join("target/gen.rs"), table_source("skipped", 99))?;
        fs::write_string(
            root.join("docs/x/Cargo.toml"),
            "f = [\"128-column-tables\"]\n",
        )?;
        fs::write_string(root.join("Cargo.toml"), "f = [\"64-column-tables\"]\n")?;
        let failures = check_tree(root)?;
        assert_eq!(
            failures,
            [
                "src/schema.rs:1: `wide` has 33 columns. A new table has 32 or fewer. \
                 Split it, or move columns out.",
                "Cargo.toml: enables `64-column-tables`. Every table fits a narrower tier, \
                 so remove the feature.",
            ]
        );
        Ok(())
    }

    #[test]
    fn the_walk_sorts_by_component() -> Result<()> {
        let dir = fs::temp_dir()?;
        let root = dir.path();
        std::fs::create_dir_all(root.join("a"))?;
        fs::write_string(root.join("a/b.rs"), "")?;
        fs::write_string(root.join("a-c.rs"), "")?;
        let found = walk(root, ".rs");
        assert_eq!(found, [PathBuf::from("a/b.rs"), PathBuf::from("a-c.rs")]);
        Ok(())
    }
}
