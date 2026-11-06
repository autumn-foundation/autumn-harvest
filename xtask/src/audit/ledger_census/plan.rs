//! The `plan` step: plan each statement, and write the plan pages.
//!
//! `plan` runs `EXPLAIN (GENERIC_PLAN, FORMAT JSON)` for each statement on a
//! migrated database, with `enable_seqscan = off`. The setting makes the
//! planner take an index whenever one can serve the predicate. A `Seq Scan`
//! that remains means that no index can, whatever the size of the table. So
//! the access path does not depend on fixture data.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use regex::Regex;
use serde_json::Value;
use smop::prelude::*;

use super::attribute::{Entry, Patterns, Statements};
use super::{FULL_INDEX, NO_INDEX, PARTIAL_INDEX, PLANS, SQLITE_SECTION, STATEMENTS};
use crate::audit::ledger_profiles::scan::CATALOG;

/// One access path: a table and how the plan reaches it.
pub type Access = (String, String);

/// One planned statement of a section.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    /// The statement text.
    pub sql: String,
    /// The sorted access paths, or `None` when the statement did not plan.
    pub paths: Option<Vec<Access>>,
    /// The first error line, when the statement did not plan.
    pub error: Option<String>,
    /// Whether the row comes from the SQLite planner.
    pub sqlite: bool,
}

/// One section of a plan page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Section {
    /// Rows that this run planned.
    Rows(Vec<Row>),
    /// A section kept verbatim from an earlier run, without its `## ` prefix.
    Raw(String),
}

/// Whether a section, new or kept from an earlier run, plans SQLite.
pub fn is_sqlite(section: &Section) -> bool {
    match section {
        Section::Raw(block) => block.contains(SQLITE_SECTION),
        Section::Rows(rows) => rows.iter().any(|row| row.sqlite),
    }
}

/// Whether no index can serve the access, whatever the statistics.
pub fn unindexed(access: &str) -> bool {
    access == NO_INDEX
}

/// Returns the index names under a bitmap heap scan, without its subplans.
fn bitmap_indexes(node: &Value) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for child in plans_of(node) {
        let kind = child.get("Node Type").and_then(Value::as_str).unwrap_or("");
        if !kind.starts_with("Bitmap") {
            continue;
        }
        if let Some(name) = child.get("Index Name") {
            names.insert(value_text(name));
        }
        names.extend(bitmap_indexes(child));
    }
    names
}

fn plans_of(node: &Value) -> &[Value] {
    node.get("Plans")
        .and_then(Value::as_array)
        .map_or(&[], Vec::as_slice)
}

/// Returns the Python `str` of a JSON scalar, as an f-string shows it.
fn value_text(value: &Value) -> String {
    value
        .as_str()
        .map_or_else(|| value.to_string(), ToString::to_string)
}

fn walk(node: &Value, out: &mut BTreeSet<Access>, partial: &BTreeSet<String>) {
    let relation = node
        .get("Relation Name")
        .filter(|r| !matches!(r, Value::Null) && r.as_str() != Some(""));
    if let Some(relation) = relation {
        let kind = node.get("Node Type").map_or_else(String::new, value_text);
        let index = node.get("Index Name").map(value_text);
        let access = if kind == "Seq Scan" {
            NO_INDEX.to_string()
        } else if let Some(index) = index.as_ref().filter(|_| node.get("Index Cond").is_none()) {
            let label = if partial.contains(index) {
                PARTIAL_INDEX
            } else {
                FULL_INDEX
            };
            format!("{label} `{index}`")
        } else if let Some(index) = index {
            format!("{kind} on `{index}`")
        } else if kind == "Bitmap Heap Scan" {
            let names: Vec<String> = bitmap_indexes(node)
                .into_iter()
                .map(|n| format!("`{n}`"))
                .collect();
            format!("{kind} on {}", names.join(", "))
        } else {
            kind
        };
        out.insert((value_text(relation), access));
    }
    for child in plans_of(node) {
        walk(child, out, partial);
    }
}

/// Returns the sorted `(table, access)` pairs of an EXPLAIN JSON plan.
pub fn access_paths(plan_json: &str, partial: &BTreeSet<String>) -> Result<Vec<Access>> {
    let plans: Value = serde_json::from_str(plan_json).context("parsing the EXPLAIN JSON")?;
    let mut found = BTreeSet::new();
    for entry in plans.as_array().context("the EXPLAIN JSON is not a list")? {
        let plan = entry.get("Plan").context("an EXPLAIN entry has no Plan")?;
        walk(plan, &mut found, partial);
    }
    Ok(found.into_iter().collect())
}

/// Returns the names of the partial indexes, whose predicate bounds a full scan.
fn partial_indexes(url: &str) -> Result<BTreeSet<String>> {
    let out = sh::cmd("psql")
        .args(["-X", "-q", "-A", "-t", url, "-c"])
        .arg(
            "SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
             WHERE i.indpred IS NOT NULL",
        )
        .output()?;
    Ok(out.split_whitespace().map(ToString::to_string).collect())
}

/// Returns `(paths, error)` for one statement.
///
/// The script goes to `psql` on stdin. `smop::sh` has no stdin input, so
/// this one call uses `std::process` directly.
fn explain(
    url: &str,
    sql: &str,
    partial: &BTreeSet<String>,
) -> Result<(Option<Vec<Access>>, Option<String>)> {
    let script = format!(
        "BEGIN;\nSET LOCAL enable_seqscan = off;\nEXPLAIN (GENERIC_PLAN, FORMAT JSON) {};\nROLLBACK;\n",
        sql.trim_end().trim_end_matches(';')
    );
    let mut child = Command::new("psql")
        .args(["-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1", url])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("starting psql")?;
    child
        .stdin
        .take()
        .context("opening the psql stdin")?
        .write_all(script.as_bytes())
        .context("writing the EXPLAIN script to psql")?;
    let done = child.wait_with_output().context("waiting for psql")?;
    if !done.status.success() {
        let stderr = String::from_utf8_lossy(&done.stderr);
        let first = stderr.trim().lines().next().map(ToString::to_string);
        return Ok((None, Some(first.unwrap_or_else(|| "psql failed".into()))));
    }
    let stdout = String::from_utf8_lossy(&done.stdout);
    let text = stdout.trim();
    let start = text.find('[').context("psql printed no EXPLAIN JSON")?;
    Ok((Some(access_paths(&text[start..], partial)?), None))
}

/// Plans one statement once for each set of bind literals that it saw.
///
/// The union of the access paths is the result. A literal set that does not
/// plan is tried again with type casts, then without its literals.
fn plan_statement(
    url: &str,
    sql: &str,
    entry: &Entry,
    partial: &BTreeSet<String>,
    patterns: &Patterns,
) -> Result<(Option<Vec<Access>>, Option<String>)> {
    let mut paths: BTreeSet<Access> = BTreeSet::new();
    let mut errors = Vec::new();
    let types: HashMap<String, String> = entry
        .types
        .iter()
        .map(|(n, t)| (format!("::{n}"), t.clone()))
        .collect();
    let mut sets: Vec<&BTreeMap<String, String>> =
        entry.literals.iter().filter(|m| !m.is_empty()).collect();
    let empty = BTreeMap::new();
    if sets.is_empty() {
        sets.push(&empty);
    }
    for literal_set in sets {
        let mut lits = types.clone();
        lits.extend(literal_set.iter().map(|(k, v)| (k.clone(), v.clone())));
        let none = HashMap::new();
        let texts = [
            patterns.substitute(sql, &lits, false),
            patterns.substitute(sql, &lits, true),
            patterns.substitute(sql, &none, false),
            patterns.substitute(sql, &types, true),
        ];
        let mut outcome = (None, None);
        for text in &texts {
            outcome = explain(url, text, partial)?;
            if outcome.1.is_none() {
                break;
            }
        }
        match outcome {
            (_, Some(error)) => errors.push(error),
            (found, None) => paths.extend(found.unwrap_or_default()),
        }
    }
    if paths.is_empty()
        && let Some(first) = errors.into_iter().next()
    {
        return Ok((None, Some(first)));
    }
    Ok((Some(paths.into_iter().collect()), None))
}

/// Returns the flat statement text, cut to `width` characters.
fn first_line(sql: &str, width: usize, space: &Regex) -> String {
    let flat = space.replace_all(sql, " ").trim().to_string();
    if flat.chars().count() <= width {
        return flat;
    }
    let head: String = flat.chars().take(width - 3).collect();
    format!("{head}...")
}

/// Renders one generated page. `results` maps a profile key to its section.
pub fn render_page(source_path: &str, results: &BTreeMap<String, Section>) -> Result<String> {
    let space = Regex::new(r"\s+").context("compiling the space pattern")?;
    let sqlite = results.values().any(is_sqlite);
    let postgres = results.values().any(|s| !is_sqlite(s));
    let mut out = vec![
        format!("# Ledger plans: `{source_path}`"),
        String::new(),
        "Generated by `cargo xtask audit ledger-census plan` (issue #2366). Do not edit.".into(),
    ];
    if postgres {
        let each = if sqlite {
            "Each Postgres statement"
        } else {
            "Each statement"
        };
        out.extend([
            format!("{each} was planned with `EXPLAIN (GENERIC_PLAN)` and"),
            "`enable_seqscan = off`. Each bool or string bind value that the census saw".into(),
            "is put in place of its parameter, as in a custom plan, so a partial index".into(),
            "on `NOT fired` applies.".into(),
        ]);
    }
    if sqlite {
        let each = if postgres {
            "Each SQLite statement"
        } else {
            "Each statement"
        };
        out.extend([
            format!("{each} was planned with `EXPLAIN QUERY PLAN`, on the schema"),
            "that the census recorded (issue #2429). SQLite has no `enable_seqscan`. With".into(),
            "no statistics it assumes a large table, so it takes any index that can serve".into(),
            "the predicate. A `SCAN` that names no index is the same finding.".into(),
        ]);
    }
    out.extend([
        String::new(),
        format!("**{NO_INDEX}** is the finding. It means that no index can serve the"),
        "predicate on that table, so the scan grows with the table. It does not".into(),
        "depend on table statistics. The header of each section lists these tables.".into(),
        String::new(),
        "The other rows show the index that the planner chose on empty tables.".into(),
        "Production statistics can change that choice. A".into(),
        format!("**{FULL_INDEX}** or **{PARTIAL_INDEX}** row is often such a choice,"),
        "where a keyed index also exists. It is a hint, not a finding.".into(),
        "A full scan of a partial index reads only the rows that match its predicate.".into(),
        String::new(),
        format!("The full statement text is in `{STATEMENTS}`."),
        String::new(),
    ]);
    for (key, section) in results {
        match section {
            Section::Raw(block) => {
                out.push(format!("## {}", block.trim_end_matches('\n')));
                out.push(String::new());
            }
            Section::Rows(rows) => render_rows(&mut out, key, rows, is_sqlite(section), &space),
        }
    }
    Ok(out.join("\n"))
}

/// Renders the section of one profile that this run planned.
fn render_rows(out: &mut Vec<String>, key: &str, rows: &[Row], sqlite: bool, space: &Regex) {
    let flags: BTreeSet<&str> = rows
        .iter()
        .flat_map(|row| row.paths.iter().flatten())
        .filter(|(_, access)| unindexed(access))
        .map(|(table, _)| table.as_str())
        .collect();
    let backend = if sqlite {
        format!(" {SQLITE_SECTION}")
    } else {
        String::new()
    };
    let flags: Vec<String> = flags.iter().map(|t| format!("`{t}`")).collect();
    let flags = if flags.is_empty() {
        "none".to_string()
    } else {
        flags.join(", ")
    };
    out.push(format!("## `{key}`"));
    out.push(String::new());
    out.push(format!(
        "Statements: {}.{backend} Tables with no usable index: {flags}.",
        rows.len()
    ));
    out.push(String::new());
    out.push("| # | Statement | Access |".into());
    out.push("|---|---|---|".into());
    for (number, row) in rows.iter().enumerate() {
        let head = first_line(&row.sql, 100, space)
            .replace('|', "\\|")
            .replace('`', "'");
        let access = match (&row.error, &row.paths) {
            (Some(error), _) if !error.is_empty() => {
                format!("not planned: {}", error.replace('|', "/"))
            }
            (_, Some(paths)) if !paths.is_empty() => {
                let parts: Vec<String> = paths.iter().map(|(t, a)| format!("`{t}`: {a}")).collect();
                parts.join("<br>")
            }
            _ => "no table".to_string(),
        };
        out.push(format!("| {} | `{head}` | {access} |", number + 1));
    }
    out.push(String::new());
}

/// Sets `plan = "<page>"` in the `["key"]` table of a catalog file's text.
pub fn set_plan_field(text: &str, key: &str, page: &str) -> Result<String> {
    let header = format!("[\"{key}\"]\n");
    let start = text
        .find(&header)
        .with_context(|| format!("the catalog has no table {header:?}"))?;
    let end = text[start + header.len()..]
        .find("\n[")
        .map_or(text.len(), |k| start + header.len() + k + 1);
    let block = &text[start..end];
    let line = format!("plan = \"{page}\"\n");
    let plan_line = Regex::new(r"(?m)^plan = .*\n").context("compiling the plan pattern")?;
    let block = if plan_line.is_match(block) {
        plan_line
            .replace_all(block, regex::NoExpand(&line))
            .into_owned()
    } else {
        let measured =
            Regex::new(r"(?m)^(measured = .*\n)").context("compiling the measured pattern")?;
        measured
            .replace_all(block, |caps: &regex::Captures<'_>| {
                format!("{}{line}", &caps[1])
            })
            .into_owned()
    };
    Ok(format!("{}{block}{}", &text[..start], &text[end..]))
}

/// Maps a catalog file to its source file.
///
/// `docs/ledger/autumn-harvest/queue.toml` maps to `autumn-harvest/src/queue.rs`.
pub fn source_of(catalog: &str) -> Result<String> {
    let rel = Path::new(catalog)
        .strip_prefix(CATALOG)
        .with_context(|| format!("{catalog} is not under {CATALOG}"))?;
    let mut parts = rel.components();
    let krate = parts
        .next()
        .with_context(|| format!("{catalog} names no crate"))?
        .as_os_str()
        .to_string_lossy()
        .into_owned();
    let rest: PathBuf = parts.collect();
    ensure!(
        rest.file_name().is_some(),
        "{catalog} names no source file under {krate}"
    );
    Ok(format!(
        "{krate}/src/{}",
        rest.with_extension("rs").to_string_lossy()
    ))
}

/// Keeps the sections of an earlier run, so a filtered run does not drop them.
fn parse_existing(page: &Path) -> Result<BTreeMap<String, Section>> {
    let text = fs::read_string(page)?;
    let heading = Regex::new(r"(?m)^## ").context("compiling the heading pattern")?;
    let mut sections = BTreeMap::new();
    for block in heading.split(&text).skip(1) {
        let key = block
            .split('`')
            .nth(1)
            .with_context(|| format!("a section of {} has no key", page.display()))?;
        sections.insert(key.to_string(), Section::Raw(block.to_string()));
    }
    Ok(sections)
}

/// Merges `results` into the catalog's plan page, and links each profile to it.
pub fn write_plan_page(
    root: &Path,
    catalog: &str,
    results: &BTreeMap<String, Section>,
) -> Result<String> {
    let source = source_of(catalog)?;
    let rel = Path::new(catalog)
        .strip_prefix(CATALOG)
        .with_context(|| format!("{catalog} is not under {CATALOG}"))?;
    let page = Path::new(PLANS).join(rel).with_extension("md");
    let page_text = page.to_string_lossy().into_owned();
    let existing = root.join(&page);
    let mut merged = if existing.is_file() {
        parse_existing(&existing)?
    } else {
        BTreeMap::new()
    };
    merged.extend(results.iter().map(|(k, v)| (k.clone(), v.clone())));
    if let Some(parent) = existing.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write_string(&existing, format!("{}\n", render_page(&source, &merged)?))?;
    let catalog_file = root.join(catalog);
    let mut text = fs::read_string(&catalog_file)?;
    for key in results.keys() {
        text = set_plan_field(&text, key, &page_text)?;
    }
    fs::write_string(&catalog_file, text)?;
    Ok(page_text)
}

/// Writes the index of every plan page.
pub fn write_index(root: &Path) -> Result<()> {
    let plans = root.join(PLANS);
    let mut pages: Vec<PathBuf> = Vec::new();
    if plans.is_dir() {
        for entry in walkdir::WalkDir::new(&plans).min_depth(1) {
            let entry = entry.with_context(|| format!("walking {}", plans.display()))?;
            let path = entry.path();
            let is_page = path.extension().is_some_and(|e| e == "md");
            if is_page && entry.file_name() != "README.md" {
                pages.push(path.to_path_buf());
            }
        }
    }
    // A `Path` sorts by components, as Python's `sorted(Path)` does.
    // So `autumn-harvest/` sorts before `autumn-harvest-plugin/`.
    pages.sort();
    let mut lines = vec![
        "# Ledger plans (issue #2366)".to_string(),
        String::new(),
        "Generated by `cargo xtask audit ledger-census plan`. Do not edit.".into(),
        "One page for each source file. See [the Ledger profiles](../README.md).".into(),
        String::new(),
    ];
    for page in pages {
        let rel = page
            .strip_prefix(&plans)
            .unwrap_or(&page)
            .to_string_lossy()
            .into_owned();
        let stem = rel.strip_suffix(".md").unwrap_or(&rel);
        lines.push(format!("- [`{stem}`]({rel})"));
    }
    fs::write_string(plans.join("README.md"), format!("{}\n", lines.join("\n")))
}

/// Returns whether a profile names `backend`, as Python's `in` reads the field.
///
/// A list names it as an item. A string names it as a substring.
pub fn names_backend(profile: &toml::Value, backend: &str) -> Result<bool> {
    let table = profile
        .as_table()
        .context("a profile is not a TOML table")?;
    Ok(match table.get("backend") {
        None => false,
        Some(toml::Value::Array(items)) => items.iter().any(|v| v.as_str() == Some(backend)),
        Some(toml::Value::String(text)) => text.contains(backend),
        Some(other) => bail!("a profile backend is not a list: {other}"),
    })
}

/// Returns whether a profile has the driver, or whether no driver was asked for.
pub fn has_driver(profile: &toml::Value, driver: Option<&str>) -> bool {
    driver.is_none_or(|d| profile.get("driver").and_then(toml::Value::as_str) == Some(d))
}

/// Runs the `plan` step against the database at `url`.
pub fn run_plan(root: &Path, url: &str, driver: Option<&str>) -> Result<()> {
    let statements: Statements = serde_json::from_str(&fs::read_string(root.join(STATEMENTS))?)
        .context("parsing the census statements")?;
    let catalogs = crate::audit::ledger_profiles::load_catalogs(root)?;
    let patterns = Patterns::new()?;
    let plannable = super::plannable()?;
    let partial = partial_indexes(url)?;
    let mut pages = Vec::new();
    let mut missing = Vec::new();
    let none = BTreeMap::new();
    for (catalog, keys) in &catalogs {
        let mut wanted = Vec::new();
        for (key, profile) in keys {
            if names_backend(profile, "postgres")? && has_driver(profile, driver) {
                wanted.push(key);
            }
        }
        wanted.sort();
        if wanted.is_empty() {
            continue;
        }
        let seen = statements.get(catalog).unwrap_or(&none);
        let mut results = BTreeMap::new();
        for key in wanted {
            let sqls: Vec<(&String, &Entry)> = seen
                .get(key)
                .into_iter()
                .flatten()
                .filter(|(sql, entry)| plannable.is_match(sql) && !entry.has_backend())
                .collect();
            if sqls.is_empty() {
                missing.push(format!("{catalog} {key}"));
                continue;
            }
            let mut rows = Vec::new();
            for (sql, entry) in sqls {
                let (paths, error) = plan_statement(url, sql, entry, &partial, &patterns)?;
                rows.push(Row {
                    sql: sql.clone(),
                    paths,
                    error,
                    sqlite: false,
                });
            }
            results.insert(key.clone(), Section::Rows(rows));
        }
        if results.is_empty() {
            continue;
        }
        pages.push(write_plan_page(root, catalog, &results)?);
    }
    write_index(root)?;
    println!(
        "ledger-census: planned {} pages; {} profiles not reached by the suites",
        pages.len(),
        missing.len()
    );
    for item in missing {
        println!("  not reached: {item}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_of_refuses_a_path_outside_the_catalog() {
        assert!(source_of("docs/other/x.toml").is_err());
        assert!(source_of("docs/ledger/x.toml").is_err());
    }

    #[test]
    fn set_plan_field_needs_the_table() {
        assert!(set_plan_field("[\"a\"]\nmeasured = \"unmeasured\"\n", "b", "p.md").is_err());
    }

    #[test]
    fn a_kept_section_renders_verbatim() -> Result<()> {
        let results = BTreeMap::from([(
            "k".to_string(),
            Section::Raw(
                "`k`\n\nStatements: 1. Backend: SQLite. Tables with no usable index: none.\n\n"
                    .into(),
            ),
        )]);
        let page = render_page("x/src/y.rs", &results)?;
        assert!(page.contains("## `k`\n\nStatements: 1."));
        assert!(page.contains("EXPLAIN QUERY PLAN"));
        assert!(!page.contains("GENERIC_PLAN"));
        Ok(())
    }

    #[test]
    fn names_backend_reads_a_list_or_a_string() -> Result<()> {
        let list: toml::Value = toml::from_str("backend = [\"postgres\"]")?;
        let text: toml::Value = toml::from_str("backend = \"postgres\"")?;
        let none: toml::Value = toml::from_str("driver = \"setup\"")?;
        assert!(names_backend(&list, "postgres")?);
        assert!(names_backend(&text, "postgres")?);
        assert!(!names_backend(&none, "postgres")?);
        assert!(has_driver(&none, Some("setup")) && !has_driver(&none, Some("per_task")));
        Ok(())
    }
}
