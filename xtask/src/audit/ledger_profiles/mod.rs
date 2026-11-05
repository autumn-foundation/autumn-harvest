//! `cargo xtask audit ledger-profiles`: fail on a SQL query that has no Ledger profile.
//!
//! This is the port of `docs/audits/ledger-profiles.py`.
//!
//! Ledger is the query-cost pass of this repository. It measures what a query
//! costs in shared buffers, and it records the result in a `docs/performance-*`
//! page. Before this gate, only the queries that Ledger chose to measure had
//! any record. A new query could ship with no record of what it reads, what it
//! writes, or how often it runs.
//!
//! A Ledger profile is that record (issue #2364). It is a TOML table in
//! `docs/ledger/`. The catalog has one file for each source file, at the same
//! path under the crate name. The profile of `enqueue` in
//! `autumn-harvest/src/queue.rs` is the table `["enqueue"]` in
//! `docs/ledger/autumn-harvest/queue.toml`. See `docs/ledger/README.md` for
//! the fields.
//!
//! A query site is a call that sends SQL to Postgres or SQLite: `sql_query`,
//! the Diesel DSL (`load`, `get_result`, `execute` and the others), or a
//! `rusqlite` call (`prepare`, `query_row`, `execute_batch` and the others).
//! The profile belongs to the function that holds the call. Code under
//! `#[cfg(test)]` and `#[test]` is out of scope.
//!
//! The gate reports these findings:
//!
//! - a function with a query site and no profile
//! - a profile with no matching function, so a rename cannot leave a stale one
//! - a catalog file with no source file
//! - a `driver` that is not one of [`DRIVERS`]
//! - a `backend`, `kind` or `tables` list that omits what the scan detects
//! - a `measured` page that does not exist or does not name the function
//! - a `plan` page that does not exist or has no section for the profile
//!
//! The scan detects `backend`, `kind` and `tables`. A profile must list at
//! least what the scan detects. It can list more, because the scan cannot
//! follow a query that another function builds. The `driver` is the one field
//! that only the author can set: what makes the query run, and so how often.
//!
//! The scan covers the shipped `src/` trees in [`CRATES`] only. This crate is
//! not shipped, and it sends no SQL, so the gate does not scan `xtask/src`.
//!
//! Usage:
//!
//! ```text
//! cargo xtask audit ledger-profiles
//! cargo xtask audit ledger-profiles --scaffold   # print stubs for missing profiles
//! cargo xtask audit ledger-profiles --self-test
//! ```

pub mod pyrepr;
pub mod scan;
mod selftest;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::process::ExitCode;

use smop::prelude::*;

use crate::util::{gate_exit, repo_root};
use scan::{CATALOG, Scanner, Site, catalog_path, lex};

/// The crates whose `src/` trees ship queries.
///
/// `autumn-harvest-redis` sends Redis commands, not SQL. `autumn-harvest-macros`
/// generates calls into the engine API, and the engine functions hold the
/// queries.
pub const CRATES: [&str; 5] = [
    "autumn-harvest",
    "autumn-harvest-plugin",
    "autumn-harvest-sqlite",
    "autumn-harvest-cli",
    "autumn-harvest-agent",
];

/// The allowed drivers, sorted, with what each one means.
pub const DRIVERS: [(&str, &str); 5] = [
    ("per_request", "runs for each API, UI, CLI or client call"),
    (
        "per_task",
        "runs for each task claim, completion, heartbeat or decision commit",
    ),
    (
        "per_tick",
        "runs on each pass of a background loop, scanner or sweeper",
    ),
    (
        "setup",
        "runs at startup, in a migration, or in an operator maintenance operation",
    ),
    (
        "test_support",
        "runs only in a test or verification harness",
    ),
];

const KINDS: [&str; 3] = ["read", "write", "ddl"];
const BACKENDS: [&str; 2] = ["postgres", "sqlite"];
const FIELDS: [&str; 6] = ["driver", "backend", "kind", "tables", "measured", "plan"];
const UNMEASURED: &str = "unmeasured";

/// Arguments of `cargo xtask audit ledger-profiles`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {
    /// Print a TOML stub for each function that has no profile yet.
    #[arg(long)]
    pub scaffold: bool,
    /// Run the fixtures of the scan and the gate, then exit.
    #[arg(long)]
    pub self_test: bool,
}

/// The source files and parsed catalogs of one run.
pub struct Repo {
    /// Each source path, relative to the root, mapped to its text.
    pub sources: BTreeMap<String, String>,
    /// Each catalog path, relative to the root, mapped to its parsed TOML.
    pub catalogs: BTreeMap<String, toml::Table>,
}

/// Runs `cargo xtask audit ledger-profiles`.
pub fn run(args: Args) -> Result<ExitCode> {
    let Args {
        scaffold: stubs,
        self_test,
    } = args;
    let scanner = Scanner::new()?;
    if self_test {
        return Ok(match selftest::self_test(&scanner) {
            Ok(()) => {
                println!("ledger-profiles self-test: OK");
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("ledger-profiles self-test: FAILED: {error:#}");
                ExitCode::FAILURE
            }
        });
    }
    let repo = load_repo(&repo_root())?;
    if stubs {
        println!("{}", scaffold(&scanner, &repo.sources, &repo.catalogs));
        return Ok(ExitCode::SUCCESS);
    }
    let root = repo_root();
    let read_text = |rel: &str| read_repo_text(&root, rel);
    let found = findings(&scanner, &repo.sources, &repo.catalogs, &read_text)?;
    for message in &found {
        println!("{message}");
    }
    let profiles: usize = repo.catalogs.values().map(toml::Table::len).sum();
    println!(
        "ledger-profiles: {profiles} profiles in {} files, {} findings",
        repo.catalogs.len(),
        found.len()
    );
    if !found.is_empty() {
        println!(
            "Fix: add or update the profile in docs/ledger/. \
             `--scaffold` prints a stub for each missing one. See docs/ledger/README.md."
        );
    }
    Ok(gate_exit(found.is_empty()))
}

/// Reads a repository file, or returns `None` when it is not a file.
pub fn read_repo_text(root: &Path, rel: &str) -> Result<Option<String>> {
    let path = root.join(rel);
    if !path.is_file() {
        return Ok(None);
    }
    fs::read_string(&path).map(Some)
}

/// Reads every shipped source file, migration and catalog file under `root`.
pub fn load_repo(root: &Path) -> Result<Repo> {
    let mut sources = BTreeMap::new();
    for krate in CRATES {
        for path in files_under(&root.join(krate).join("src"), |name| {
            Path::new(name).extension().is_some_and(|e| e == "rs")
        })? {
            sources.insert(relative(root, &path), fs::read_string(&path)?);
        }
    }
    let mut children: Vec<_> = std::fs::read_dir(root)
        .with_context(|| format!("listing {}", root.display()))?
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .collect();
    children.sort();
    for child in children {
        let migrations = child.join("migrations");
        if !migrations.is_dir() {
            continue;
        }
        for path in files_under(&migrations, |name| name == "up.sql")? {
            sources.insert(relative(root, &path), fs::read_string(&path)?);
        }
    }
    let catalogs = load_catalogs(root)?;
    Ok(Repo { sources, catalogs })
}

/// Parses every catalog file under `root/docs/ledger`.
pub fn load_catalogs(root: &Path) -> Result<BTreeMap<String, toml::Table>> {
    let mut catalogs = BTreeMap::new();
    for path in files_under(&root.join(CATALOG), |name| {
        Path::new(name).extension().is_some_and(|e| e == "toml")
    })? {
        let text = fs::read_string(&path)?;
        let table: toml::Table =
            toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        catalogs.insert(relative(root, &path), table);
    }
    Ok(catalogs)
}

/// Returns each file under `dir` whose name passes `keep`, sorted.
///
/// A missing directory has no files, as `Path.rglob` has none.
fn files_under(dir: &Path, keep: impl Fn(&str) -> bool) -> Result<Vec<std::path::PathBuf>> {
    let mut out = Vec::new();
    if !dir.is_dir() {
        return Ok(out);
    }
    for entry in walkdir::WalkDir::new(dir).min_depth(1) {
        let entry = entry.with_context(|| format!("walking {}", dir.display()))?;
        let name = entry.file_name().to_string_lossy();
        if keep(&name) && entry.path().is_file() {
            out.push(entry.path().to_path_buf());
        }
    }
    out.sort();
    Ok(out)
}

/// Returns `path` relative to `root`, with `/` separators.
fn relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

/// Checks one profile against the sites that the scan detected.
fn check_profile(
    place: &str,
    key: &str,
    profile: &toml::Value,
    detected: &Site,
    read_text: &dyn Fn(&str) -> Result<Option<String>>,
) -> Result<Vec<String>> {
    let Some(profile) = profile.as_table() else {
        bail!("{place}: [\"{key}\"] is not a TOML table");
    };
    let mut found = Vec::new();
    let unknown: Vec<&str> = profile
        .keys()
        .map(String::as_str)
        .filter(|k| !FIELDS.contains(k))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if !unknown.is_empty() {
        found.push(format!(
            "{place}: [\"{key}\"] has unknown field(s) {}",
            pyrepr::list_repr(&unknown)
        ));
    }
    let driver = profile.get("driver");
    if matches!(driver, Some(toml::Value::Array(_) | toml::Value::Table(_))) {
        bail!("{place}: [\"{key}\"] driver is a list or a table, which Python cannot hash");
    }
    let known_driver = driver
        .and_then(toml::Value::as_str)
        .is_some_and(|d| DRIVERS.iter().any(|(name, _)| *name == d));
    if !known_driver {
        let names: Vec<&str> = DRIVERS.iter().map(|(name, _)| *name).collect();
        found.push(format!(
            "{place}: [\"{key}\"] driver {} is not one of {}",
            pyrepr::opt_toml_repr(driver),
            pyrepr::list_repr(&names)
        ));
    }
    let lists: [(&str, Option<&[&str]>); 3] = [
        ("backend", Some(&BACKENDS)),
        ("kind", Some(&KINDS)),
        ("tables", None),
    ];
    for (field, allowed) in lists {
        let values: Option<Vec<&str>> = profile
            .get(field)
            .and_then(toml::Value::as_array)
            .and_then(|items| items.iter().map(toml::Value::as_str).collect());
        let Some(values) = values else {
            found.push(format!(
                "{place}: [\"{key}\"] {field} must be a list of strings"
            ));
            continue;
        };
        if let Some(allowed) = allowed {
            let bad: Vec<&str> = values
                .iter()
                .copied()
                .filter(|v| !allowed.contains(v))
                .collect();
            if !bad.is_empty() {
                found.push(format!(
                    "{place}: [\"{key}\"] {field} {} is not one of {}",
                    pyrepr::list_repr(&bad),
                    pyrepr::list_repr(allowed)
                ));
            }
        }
        let missing: Vec<&str> = detected
            .field(field)
            .iter()
            .map(String::as_str)
            .filter(|d| !values.contains(d))
            .collect();
        if !missing.is_empty() {
            found.push(format!(
                "{place}: [\"{key}\"] {field} omits detected {}",
                pyrepr::list_repr(&missing)
            ));
        }
    }
    found.extend(check_pages(place, key, profile, read_text)?);
    Ok(found)
}

/// Checks the `measured` and `plan` pages of one profile.
fn check_pages(
    place: &str,
    key: &str,
    profile: &toml::Table,
    read_text: &dyn Fn(&str) -> Result<Option<String>>,
) -> Result<Vec<String>> {
    let mut found = Vec::new();
    match profile.get("measured") {
        Some(toml::Value::String(measured)) if !measured.is_empty() => {
            if measured != UNMEASURED {
                let page = measured.split('#').next().unwrap_or("");
                let last = key.rsplit("::").next().unwrap_or(key);
                let name = last.trim_end_matches('!');
                match read_text(page)? {
                    None => found.push(format!(
                        "{place}: [\"{key}\"] measured page {page} does not exist"
                    )),
                    Some(text) => {
                        let pattern = regex::Regex::new(&format!(r"\b{}\b", regex::escape(name)))
                            .context("compiling the measured-name pattern")?;
                        if !pattern.is_match(&text) {
                            found.push(format!(
                                "{place}: [\"{key}\"] measured page {page} does not name `{name}`"
                            ));
                        }
                    }
                }
            }
        }
        _ => found.push(format!(
            "{place}: [\"{key}\"] measured must be {} or a docs page",
            pyrepr::str_repr(UNMEASURED)
        )),
    }
    if let Some(plan) = profile.get("plan") {
        let text = match plan {
            toml::Value::String(page) => read_text(page)?,
            _ => None,
        };
        let shown = pyrepr::toml_str(plan);
        match text {
            None => found.push(format!(
                "{place}: [\"{key}\"] plan page {shown} does not exist"
            )),
            Some(text) if !text.contains(&format!("## `{key}`")) => found.push(format!(
                "{place}: [\"{key}\"] plan page {shown} has no section for `{key}`"
            )),
            Some(_) => {}
        }
    }
    Ok(found)
}

/// Compares scanned query sites with the catalog, and returns one message per defect.
///
/// `sources` maps a source path to its text. `catalogs` maps a catalog path to
/// its parsed TOML. `read_text` returns a repository file's text, or `None`.
pub fn findings(
    scanner: &Scanner,
    sources: &BTreeMap<String, String>,
    catalogs: &BTreeMap<String, toml::Table>,
    read_text: &dyn Fn(&str) -> Result<Option<String>>,
) -> Result<Vec<String>> {
    let tables = scanner.known_tables(sources.iter().map(|(p, s)| (p.as_str(), s.as_str())));
    let mut excluded = BTreeSet::new();
    for (path, source) in sources {
        let frames = scanner.frames_of(&lex(source).0);
        let path = Path::new(path);
        let is_root = path
            .file_name()
            .is_some_and(|n| n == "lib.rs" || n == "main.rs" || n == "mod.rs");
        let base = if is_root {
            path.parent().unwrap_or_else(|| Path::new("")).to_path_buf()
        } else {
            path.with_extension("")
        };
        for name in &frames.test_mods {
            excluded.insert(
                base.join(format!("{name}.rs"))
                    .to_string_lossy()
                    .into_owned(),
            );
            excluded.insert(
                base.join(name)
                    .join("mod.rs")
                    .to_string_lossy()
                    .into_owned(),
            );
        }
    }
    let empty = toml::Table::new();
    let mut found = Vec::new();
    let mut expected = BTreeSet::new();
    for (path, source) in sources {
        if excluded.contains(path) || !path.contains("/src/") {
            continue;
        }
        let detected = scanner.scan_source(source, &tables);
        let place = catalog_path(path);
        if detected.is_empty() && !catalogs.contains_key(&place) {
            continue;
        }
        expected.insert(place.clone());
        let catalog = catalogs.get(&place).unwrap_or(&empty);
        for (key, entry) in &detected {
            let Some(profile) = catalog.get(key) else {
                found.push(format!(
                    "{place}: no profile for `{key}` ({path}:{})",
                    entry.line
                ));
                continue;
            };
            found.extend(check_profile(&place, key, profile, entry, read_text)?);
        }
        let stale: BTreeSet<&String> = catalog
            .keys()
            .filter(|k| !detected.contains_key(*k))
            .collect();
        for key in stale {
            found.push(format!(
                "{place}: profile [\"{key}\"] matches no query site in {path}"
            ));
        }
    }
    for place in catalogs.keys().filter(|c| !expected.contains(*c)) {
        found.push(format!(
            "{place}: no source file has query sites for this catalog"
        ));
    }
    Ok(found)
}

/// Returns TOML stubs for each function that has no profile yet.
pub fn scaffold(
    scanner: &Scanner,
    sources: &BTreeMap<String, String>,
    catalogs: &BTreeMap<String, toml::Table>,
) -> String {
    let tables = scanner.known_tables(sources.iter().map(|(p, s)| (p.as_str(), s.as_str())));
    let empty = toml::Table::new();
    let mut out = Vec::new();
    for (path, source) in sources {
        let place = catalog_path(path);
        let catalog = catalogs.get(&place).unwrap_or(&empty);
        let missing: Vec<(String, Site)> = scanner
            .scan_source(source, &tables)
            .into_iter()
            .filter(|(key, _)| !catalog.contains_key(key))
            .collect();
        if !missing.is_empty() {
            out.push(format!("# {place}"));
            out.extend(
                missing
                    .iter()
                    .map(|(key, entry)| render(key, entry, "CHOOSE")),
            );
        }
    }
    out.join("\n")
}

/// Renders one profile stub.
fn render(key: &str, entry: &Site, driver: &str) -> String {
    let items = |values: &BTreeSet<String>| {
        let quoted: Vec<String> = values.iter().map(|v| format!("\"{v}\"")).collect();
        format!("[{}]", quoted.join(", "))
    };
    format!(
        "[\"{key}\"]\ndriver = \"{driver}\"\nbackend = {}\nkind = {}\ntables = {}\nmeasured = \"{UNMEASURED}\"\n",
        items(&entry.backend),
        items(&entry.kind),
        items(&entry.tables),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_self_test_fixtures_pass() -> Result<()> {
        selftest::self_test(&Scanner::new()?)
    }

    #[test]
    fn a_profile_that_is_not_a_table_is_an_error() {
        let site = Site {
            line: 1,
            backend: BTreeSet::new(),
            kind: BTreeSet::new(),
            tables: BTreeSet::new(),
        };
        let no_pages = |_: &str| -> Result<Option<String>> { Ok(None) };
        let result = check_profile("c.toml", "k", &toml::Value::Integer(1), &site, &no_pages);
        assert!(result.is_err());
    }

    #[test]
    fn render_sorts_each_list() {
        let site = Site {
            line: 3,
            backend: BTreeSet::from(["postgres".to_string()]),
            kind: BTreeSet::from(["write".to_string(), "read".to_string()]),
            tables: BTreeSet::new(),
        };
        assert_eq!(
            render("k", &site, "CHOOSE"),
            "[\"k\"]\ndriver = \"CHOOSE\"\nbackend = [\"postgres\"]\nkind = [\"read\", \"write\"]\n\
             tables = []\nmeasured = \"unmeasured\"\n"
        );
    }

    #[test]
    fn the_scan_never_reads_this_crate() -> Result<()> {
        let repo = load_repo(&repo_root())?;
        assert!(!repo.sources.is_empty());
        assert!(!repo.sources.keys().any(|p| p.starts_with("xtask/")));
        assert!(repo.sources.keys().any(|p| p.ends_with("/up.sql")));
        Ok(())
    }
}
