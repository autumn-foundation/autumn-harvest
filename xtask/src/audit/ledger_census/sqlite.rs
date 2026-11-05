//! The `plan-sqlite` step: plan each SQLite statement.
//!
//! It does the same as `plan` for SQLite statements, with `EXPLAIN QUERY PLAN`
//! on an in-memory database (issue #2429). The census recorded the SQLite DDL,
//! and the step replays it into that database. A `SCAN` that names no index is
//! the finding.
//!
//! The Python script used the `sqlite3` module. This port uses `rusqlite`, and
//! it raises the same binding errors that the `sqlite3` module raises. The
//! retry on a wrong parameter count reads the count from that error text.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use regex::Regex;
use rusqlite::Connection;
use smop::prelude::*;

use super::attribute::Statements;
use super::plan::{Access, Row, Section, has_driver, names_backend, write_index, write_plan_page};
use super::{FULL_INDEX, NO_INDEX, PARTIAL_INDEX, STATEMENTS};

/// DDL kinds in replay order. A table must exist before its column, index or trigger.
pub const SQLITE_DDL: [&str; 7] = [
    "CREATE TABLE",
    "CREATE VIRTUAL TABLE",
    "ALTER TABLE",
    "CREATE INDEX",
    "CREATE UNIQUE INDEX",
    "CREATE VIEW",
    "CREATE TRIGGER",
];

const SQL_KEYWORDS: [&str; 20] = [
    "WHERE",
    "ON",
    "JOIN",
    "LEFT",
    "INNER",
    "CROSS",
    "SET",
    "GROUP",
    "ORDER",
    "LIMIT",
    "USING",
    "NATURAL",
    "UNION",
    "VALUES",
    "RETURNING",
    "AND",
    "OR",
    "AS",
    "INDEXED",
    "NOT",
];

/// The compiled patterns of the SQLite planner.
pub struct SqlitePatterns {
    scan: Regex,
    search: Regex,
    head: Regex,
    bare: Regex,
    ddl_prefix: Regex,
    space: Regex,
    where_word: Regex,
    alias_after: Regex,
    alias_comma: Regex,
    named: Regex,
    numbered: Regex,
    digit_start: Regex,
    uses: Regex,
}

impl SqlitePatterns {
    /// Compiles every pattern.
    pub fn new() -> Result<Self> {
        let compile = |p: &str| Regex::new(p).with_context(|| format!("compiling the pattern {p}"));
        Ok(Self {
            scan: compile(r"^SCAN (\w+)(?: AS \w+)?(?: USING (?:COVERING )?INDEX (\w+))?")?,
            search: compile(
                r"^SEARCH (\w+)(?: AS \w+)? USING (?:(?:COVERING )?INDEX (\w+)|INTEGER PRIMARY KEY|PRIMARY KEY)",
            )?,
            head: compile(r"^(SEARCH|SCAN) (\w+)(?: AS (\w+))?")?,
            bare: compile(r"^SEARCH (\w+)(?: AS \w+)?\z")?,
            ddl_prefix: compile(r"(?s)^(?:\s|--[^\n]*\n|/\*.*?\*/)*")?,
            space: compile(r"\s+")?,
            where_word: compile(r"(?i)\bWHERE\b")?,
            alias_after: compile(r"(?i)\b(?:FROM|JOIN|UPDATE|INTO)\s+(\w+)\s+(?:AS\s+)?(\w+)")?,
            alias_comma: compile(r",\s*(\w+)\s+(?:AS\s+)?(\w+)")?,
            named: compile(r"[:@$][A-Za-z_]\w*")?,
            numbered: compile(r"\?(\d+)")?,
            digit_start: compile(r"^\d")?,
            uses: compile(r"uses (\d+)")?,
        })
    }

    /// Returns the replay rank of a DDL statement, or `None` for any other statement.
    pub fn ddl_kind(&self, sql: &str) -> Option<usize> {
        let head = self.ddl_prefix.replace(sql, "");
        let head = self.space.replace_all(&head, " ").to_uppercase();
        let head = head.replace(" IF NOT EXISTS", "");
        SQLITE_DDL.iter().position(|kind| head.starts_with(kind))
    }

    /// Maps each table alias in `sql` to its table. SQLite's plan names the alias.
    pub fn aliases(&self, sql: &str, tables: &BTreeSet<String>) -> BTreeMap<String, String> {
        let mut aliases = BTreeMap::new();
        for pattern in [&self.alias_after, &self.alias_comma] {
            for caps in pattern.captures_iter(sql) {
                let (table, alias) = (&caps[1], &caps[2]);
                let keyword = SQL_KEYWORDS.contains(&alias.to_uppercase().as_str());
                if tables.contains(table) && !keyword {
                    aliases.insert(alias.to_string(), table.to_string());
                }
            }
        }
        aliases
    }

    /// Maps one `EXPLAIN QUERY PLAN` row to `(table, access)`, or `None`.
    pub fn access(
        &self,
        detail: &str,
        tables: &BTreeSet<String>,
        partial: &BTreeSet<String>,
        aliases: &BTreeMap<String, String>,
    ) -> Option<Access> {
        let mut detail = detail.to_string();
        if let Some(caps) = self.head.captures(&detail)
            && !aliases.is_empty()
            && let Some(table) = aliases.get(&caps[2])
            && !tables.contains(&caps[2])
        {
            let end = caps.get(0).map_or(0, |m| m.end());
            detail = format!("{} {table}{}", &caps[1], &detail[end..]);
        }
        // A bare `SEARCH t` is a MIN or MAX that no index can answer. The loop
        // reads every row, so it is a scan.
        if let Some(caps) = self.bare.captures(detail.trim())
            && tables.contains(&caps[1])
        {
            return Some((caps[1].to_string(), NO_INDEX.to_string()));
        }
        if let Some(caps) = self.search.captures(&detail)
            && tables.contains(&caps[1])
        {
            let access = caps.get(2).map_or_else(
                || "Search on the primary key".to_string(),
                |index| format!("Search on `{}`", index.as_str()),
            );
            return Some((caps[1].to_string(), access));
        }
        if let Some(caps) = self.scan.captures(&detail)
            && tables.contains(&caps[1])
        {
            let Some(index) = caps.get(2) else {
                return Some((caps[1].to_string(), NO_INDEX.to_string()));
            };
            let label = if partial.contains(index.as_str()) {
                PARTIAL_INDEX
            } else {
                FULL_INDEX
            };
            return Some((caps[1].to_string(), format!("{label} `{}`", index.as_str())));
        }
        None
    }

    /// Binds NULL to each parameter, so that the planner sees an unbound value.
    pub fn params(&self, sql: &str) -> Params {
        let named: BTreeSet<String> = self
            .named
            .find_iter(sql)
            .map(|m| m.as_str()[1..].to_string())
            .collect();
        if !named.is_empty() {
            return Params::Named(named);
        }
        let numbered = self
            .numbered
            .captures_iter(sql)
            .filter_map(|caps| caps[1].parse::<usize>().ok())
            .max()
            .unwrap_or(0);
        let bare = sql
            .match_indices('?')
            .filter(|(i, _)| !self.digit_start.is_match(&sql[i + 1..]))
            .count();
        Params::Positional(numbered.max(bare))
    }

    /// Returns `(paths, error)` for one SQLite statement.
    pub fn explain(
        &self,
        db: &Connection,
        sql: &str,
        tables: &BTreeSet<String>,
        partial: &BTreeSet<String>,
    ) -> (Option<Vec<Access>>, Option<String>) {
        let text = format!(
            "EXPLAIN QUERY PLAN {}",
            sql.trim_end().trim_end_matches(';')
        );
        let rows = match plan_rows(db, &text, &self.params(sql)) {
            Ok(rows) => rows,
            Err(PlanError::Binding(message)) => {
                // A `?` inside a string literal is not a parameter. SQLite names the true count.
                let Some(uses) = self
                    .uses
                    .captures(&message)
                    .and_then(|caps| caps[1].parse::<usize>().ok())
                else {
                    return (None, Some(first_line(&message)));
                };
                match plan_rows(db, &text, &Params::Positional(uses)) {
                    Ok(rows) => rows,
                    Err(retry) => return (None, Some(first_line(retry.message()))),
                }
            }
            Err(PlanError::Sqlite(message)) => return (None, Some(first_line(&message))),
        };
        let aliases = self.aliases(sql, tables);
        let found: BTreeSet<Access> = rows
            .iter()
            .filter_map(|detail| self.access(detail, tables, partial, &aliases))
            .collect();
        (Some(found.into_iter().collect()), None)
    }
}

/// The parameters that a statement receives, as the Python script passed them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Params {
    /// A dictionary of names, each bound to NULL.
    Named(BTreeSet<String>),
    /// A list of this many NULL values.
    Positional(usize),
}

/// Why a statement did not plan.
enum PlanError {
    /// A binding error that the `sqlite3` module raises before SQLite runs.
    Binding(String),
    /// An error that SQLite reports.
    Sqlite(String),
}

impl PlanError {
    fn message(&self) -> &str {
        match self {
            Self::Binding(message) | Self::Sqlite(message) => message,
        }
    }
}

fn first_line(message: &str) -> String {
    message.lines().next().unwrap_or("").to_string()
}

/// Returns the text that the `sqlite3` module shows for a SQLite error.
fn sqlite_message(error: &rusqlite::Error) -> String {
    match error {
        rusqlite::Error::SqliteFailure(_, Some(message)) => message.clone(),
        rusqlite::Error::SqlInputError { msg, .. } => msg.clone(),
        other => other.to_string(),
    }
}

/// Runs one `EXPLAIN QUERY PLAN` and returns the detail column of each row.
fn plan_rows(db: &Connection, text: &str, params: &Params) -> Result<Vec<String>, PlanError> {
    let mut stmt = db.prepare(text).map_err(|error| match error {
        rusqlite::Error::MultipleStatement => {
            PlanError::Binding("You can only execute one statement at a time.".into())
        }
        other => PlanError::Sqlite(sqlite_message(&other)),
    })?;
    let count = stmt.parameter_count();
    match params {
        Params::Positional(supplied) => {
            if *supplied != count {
                return Err(PlanError::Binding(format!(
                    "Incorrect number of bindings supplied. The current statement uses {count}, \
                     and there are {supplied} supplied."
                )));
            }
        }
        Params::Named(names) => {
            for index in 1..=count {
                let Some(name) = stmt.parameter_name(index) else {
                    return Err(PlanError::Binding(format!(
                        "Binding {index} has no name, but you supplied a dictionary (which has only names)."
                    )));
                };
                if !names.contains(name.get(1..).unwrap_or("")) {
                    return Err(PlanError::Binding(format!(
                        "You did not supply a value for binding parameter {name}."
                    )));
                }
            }
        }
    }
    for index in 1..=count {
        stmt.raw_bind_parameter(index, rusqlite::types::Null)
            .map_err(|error| PlanError::Sqlite(sqlite_message(&error)))?;
    }
    let columns = stmt.column_count();
    let mut rows = stmt.raw_query();
    let mut details = Vec::new();
    loop {
        match rows.next() {
            Ok(Some(row)) => {
                let detail: String = row
                    .get(columns.saturating_sub(1))
                    .map_err(|error| PlanError::Sqlite(sqlite_message(&error)))?;
                details.push(detail);
            }
            Ok(None) => break,
            Err(error) => return Err(PlanError::Sqlite(sqlite_message(&error))),
        }
    }
    Ok(details)
}

/// Builds an in-memory database from every SQLite DDL statement the census saw.
///
/// The suites run the schema set-up, so the census holds it. A statement
/// that fails, such as an `ALTER TABLE` that adds a column the table has, is
/// skipped. Each run starts from a fresh schema, so it changes nothing.
pub fn schema(
    statements: &Statements,
    patterns: &SqlitePatterns,
) -> Result<(Connection, BTreeSet<String>, BTreeSet<String>)> {
    let mut ddl: BTreeSet<(usize, &str)> = BTreeSet::new();
    for keys in statements.values() {
        for sqls in keys.values() {
            for (sql, entry) in sqls {
                if entry.is_sqlite()
                    && let Some(rank) = patterns.ddl_kind(sql)
                {
                    ddl.insert((rank, sql.as_str()));
                }
            }
        }
    }
    let db = Connection::open_in_memory().context("opening an in-memory SQLite database")?;
    for (_, sql) in &ddl {
        // A statement that fails is skipped, as the doc comment explains.
        let _ = db.execute_batch(sql);
    }
    let tables = names_of(&db, "SELECT name FROM sqlite_master WHERE type = 'table'")?;
    let mut partial = BTreeSet::new();
    {
        let mut stmt = db
            .prepare("SELECT name, sql FROM sqlite_master WHERE type = 'index'")
            .context("listing the SQLite indexes")?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
            })
            .context("listing the SQLite indexes")?;
        for row in rows {
            let (name, sql) = row.context("reading a SQLite index")?;
            if sql.is_some_and(|s| patterns.where_word.is_match(&s)) {
                partial.insert(name);
            }
        }
    }
    Ok((db, tables, partial))
}

fn names_of(db: &Connection, query: &str) -> Result<BTreeSet<String>> {
    let mut stmt = db.prepare(query).context("listing the SQLite tables")?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .context("listing the SQLite tables")?;
    let mut names = BTreeSet::new();
    for row in rows {
        names.insert(row.context("reading a SQLite table name")?);
    }
    Ok(names)
}

/// Runs the `plan-sqlite` step.
pub fn run_plan_sqlite(root: &Path, driver: Option<&str>) -> Result<()> {
    let statements: Statements = serde_json::from_str(&fs::read_string(root.join(STATEMENTS))?)
        .context("parsing the census statements")?;
    let catalogs = crate::audit::ledger_profiles::load_catalogs(root)?;
    let patterns = SqlitePatterns::new()?;
    let plannable = super::plannable()?;
    let (db, tables, partial) = schema(&statements, &patterns)?;
    let mut pages = Vec::new();
    let mut missing = Vec::new();
    let no_keys = BTreeMap::new();
    for (catalog, keys) in &catalogs {
        let mut wanted = Vec::new();
        for (key, profile) in keys {
            if names_backend(profile, "sqlite")? && has_driver(profile, driver) {
                wanted.push(key);
            }
        }
        wanted.sort();
        if wanted.is_empty() {
            continue;
        }
        let seen = statements.get(catalog).unwrap_or(&no_keys);
        let mut results = BTreeMap::new();
        for key in wanted {
            let ours: Vec<&String> = seen
                .get(key)
                .into_iter()
                .flatten()
                .filter(|(_, entry)| entry.is_sqlite())
                .map(|(sql, _)| sql)
                .collect();
            let sqls: Vec<&String> = ours
                .iter()
                .copied()
                .filter(|s| plannable.is_match(s))
                .collect();
            if sqls.is_empty() {
                let note = if ours.is_empty() {
                    ""
                } else {
                    " (only DDL or pragmas)"
                };
                missing.push(format!("{catalog} {key}{note}"));
                continue;
            }
            let rows = sqls
                .into_iter()
                .map(|sql| {
                    let (paths, error) = patterns.explain(&db, sql, &tables, &partial);
                    Row {
                        sql: sql.clone(),
                        paths,
                        error,
                        sqlite: true,
                    }
                })
                .collect();
            results.insert(key.clone(), Section::Rows(rows));
        }
        if results.is_empty() {
            continue;
        }
        pages.push(write_plan_page(root, catalog, &results)?);
    }
    write_index(root)?;
    println!(
        "ledger-census: planned {} SQLite pages from {} tables; {} profiles not reached by the suites",
        pages.len(),
        tables.len(),
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
    fn a_comma_alias_is_case_sensitive() -> Result<()> {
        let patterns = SqlitePatterns::new()?;
        let tables = BTreeSet::from(["q".to_string()]);
        let upper = patterns.aliases("SELECT 1 FROM x, q AS r", &tables);
        assert_eq!(upper.get("r").map(String::as_str), Some("q"));
        let lower = patterns.aliases("SELECT 1 FROM x, q as r", &tables);
        assert!(lower.is_empty(), "{lower:?}");
        Ok(())
    }

    #[test]
    fn a_dictionary_needs_every_name() -> Result<()> {
        let patterns = SqlitePatterns::new()?;
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE q (id INTEGER)")?;
        let tables = BTreeSet::from(["q".to_string()]);
        let (paths, error) = patterns.explain(
            &db,
            "SELECT id FROM q WHERE id = :id",
            &tables,
            &BTreeSet::new(),
        );
        assert!(error.is_none(), "{error:?}");
        assert_eq!(paths, Some(vec![("q".to_string(), NO_INDEX.to_string())]));
        let (_, error) = patterns.explain(
            &db,
            "SELECT id FROM q WHERE id = ? AND :a = 1",
            &tables,
            &BTreeSet::new(),
        );
        assert_eq!(
            error.as_deref(),
            Some("Binding 1 has no name, but you supplied a dictionary (which has only names).")
        );
        Ok(())
    }
}
