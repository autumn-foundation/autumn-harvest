//! The `attribute` step: map raw census lines to profiles.
//!
//! `autumn-harvest/src/ledger_census.rs` writes the raw census while the DB
//! suites run. Each line holds one statement and the source frames of its call
//! stack. This step maps the innermost frame that falls in a profiled function
//! to that profile.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use regex::Regex;
use serde_json::Value;
use smop::prelude::*;

use super::pyjson;
use crate::audit::ledger_profiles::scan::{Frames, Scanner, catalog_path, lex};

/// A longer string is a payload, not a predicate value. It keeps the file small.
pub const MAX_LITERAL: usize = 64;
/// Literal sets kept for each statement.
pub const MAX_LITERAL_SETS: usize = 4;
/// A statement with more parameters is a bulk write.
///
/// Its guessed types would fill most of the file, and a bulk write plans
/// without them.
pub const MAX_TYPED_PARAMS: usize = 64;

/// The start of each `Debug` form that a `serde_json::Value` bind has.
const JSON_DEBUG: [&str; 6] = ["Object {", "Array [", "String(", "Number(", "Bool(", "Null"];

/// One parameter map: a number to its SQL literal, or `::n` to the guessed type of `n`.
pub type Literals = BTreeMap<String, String>;

/// The recorded shape of one statement.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(crate = "smop::serde")]
pub struct Entry {
    /// The guessed type of each parameter.
    pub types: BTreeMap<String, String>,
    /// Each distinct literal set, up to [`MAX_LITERAL_SETS`].
    pub literals: Vec<Literals>,
    /// Any other key, such as `"backend": "sqlite"`.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl Entry {
    /// Whether the entry carries a `backend` key.
    pub fn has_backend(&self) -> bool {
        self.extra.contains_key("backend")
    }

    /// Whether the entry is a SQLite statement.
    pub fn is_sqlite(&self) -> bool {
        self.extra.get("backend").and_then(Value::as_str) == Some("sqlite")
    }
}

/// The census statements: catalog, then profile key, then SQL text.
pub type Statements = BTreeMap<String, BTreeMap<String, BTreeMap<String, Entry>>>;

/// The compiled patterns of the census helpers.
pub struct Patterns {
    state_word: Regex,
    uuid: Regex,
    timestamptz: Regex,
    integer: Regex,
    float: Regex,
    parameter: Regex,
    blanks: Regex,
    newline_run: Regex,
}

impl Patterns {
    /// Compiles every pattern.
    pub fn new() -> Result<Self> {
        let compile = |p: &str| Regex::new(p).with_context(|| format!("compiling the pattern {p}"));
        // Python's `$` also matches before one final newline, so `\n?\z` stands in for it.
        Ok(Self {
            // A partial-index predicate names a state such as 'RUNNING'. Only
            // such words are kept. A test key or payload looks like a secret
            // to push protection.
            state_word: compile(r"^[A-Za-z_]{1,32}\n?\z")?,
            uuid: compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\n?\z")?,
            timestamptz: compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z\n?\z")?,
            integer: compile(r"^-?\d+\z")?,
            float: compile(r"^-?\d+\.\d+(?:e-?\d+)?\z")?,
            parameter: compile(r"\$(\d+)\b")?,
            blanks: compile(r"[ \t]+")?,
            newline_run: compile(r" ?\n[ \n]*")?,
        })
    }

    /// Collapses indentation, but keeps each newline that ends a `--` comment.
    pub fn normalize(&self, sql: &str) -> String {
        let sql = self.blanks.replace_all(sql, " ");
        self.newline_run.replace_all(&sql, "\n").trim().to_string()
    }

    /// Returns a quoted SQL literal for a Rust `Debug` string, or `None`.
    fn sql_string(&self, debug: &str) -> Option<String> {
        let Ok(Value::String(value)) = serde_json::from_str::<Value>(debug) else {
            return None;
        };
        if !self.state_word.is_match(&value) {
            return None;
        }
        Some(format!("'{value}'"))
    }

    /// Returns the SQL literal of one bind's `Debug` text, for a bool or a string.
    pub fn literal(&self, debug: &str) -> Option<String> {
        if debug == "true" || debug == "false" {
            return Some(debug.to_string());
        }
        if let Some(inner) = some_inner(debug) {
            return self.literal(inner);
        }
        if debug.starts_with('"') {
            return self.sql_string(debug);
        }
        if let Some(inner) = debug.strip_prefix('[').and_then(|d| d.strip_suffix(']')) {
            let items: Vec<Option<String>> = split_top(inner)
                .iter()
                .map(|item| {
                    if item.starts_with('"') {
                        self.sql_string(item)
                    } else {
                        None
                    }
                })
                .collect();
            if !items.is_empty() && items.iter().all(Option::is_some) {
                let items: Vec<String> = items.into_iter().flatten().collect();
                return Some(format!("ARRAY[{}]", items.join(", ")));
            }
        }
        None
    }

    /// Guesses the Postgres type of one bind from its `Debug` text, or `None`.
    ///
    /// A generic plan cannot infer the type of `$3[2]` or `unnest($1)`. The
    /// guess is used only after a plan fails, so a wrong guess costs nothing.
    pub fn bind_type(&self, debug: &str) -> Option<String> {
        if debug == "true" || debug == "false" {
            return Some("boolean".into());
        }
        if let Some(inner) = some_inner(debug) {
            return self.bind_type(inner);
        }
        if self.integer.is_match(debug) {
            return Some("bigint".into());
        }
        if self.float.is_match(debug) {
            return Some("float8".into());
        }
        if self.uuid.is_match(debug) {
            return Some("uuid".into());
        }
        if self.timestamptz.is_match(debug) {
            return Some("timestamptz".into());
        }
        if debug.starts_with('"') {
            return Some("text".into());
        }
        if JSON_DEBUG.iter().any(|prefix| debug.starts_with(prefix)) {
            return Some("jsonb".into());
        }
        if let Some(inner) = debug.strip_prefix('[').and_then(|d| d.strip_suffix(']')) {
            for item in split_top(inner) {
                match self.bind_type(&item) {
                    Some(kind) if !kind.ends_with("[]") => return Some(format!("{kind}[]")),
                    Some(_) => return None,
                    None => {}
                }
            }
        }
        None
    }

    /// Maps parameter numbers to the literals that a custom plan would see.
    ///
    /// Only bool values and state words are kept. They decide whether a
    /// partial index such as `WHERE NOT fired` applies. A generic plan cannot
    /// prove that. The key `::n` holds the guessed type of parameter `n`.
    pub fn literals(&self, binds: &str) -> Literals {
        let binds = binds.trim();
        let mut out = Literals::new();
        let Some(inner) = binds.strip_prefix('[').and_then(|b| b.strip_suffix(']')) else {
            return out;
        };
        for (number, debug) in split_top(inner).iter().enumerate() {
            let number = number + 1;
            if let Some(value) = self.literal(debug)
                && value.chars().count() <= MAX_LITERAL
            {
                out.insert(number.to_string(), value);
            }
            if let Some(kind) = self.bind_type(debug) {
                out.insert(format!("::{number}"), kind);
            }
        }
        out
    }

    /// Puts each literal in place of its parameter, then renumbers the rest.
    ///
    /// With `cast`, each remaining parameter with a guessed type gets a cast.
    pub fn substitute(&self, sql: &str, lits: &HashMap<String, String>, cast: bool) -> String {
        let mut numbers: HashMap<String, usize> = HashMap::new();
        self.parameter
            .replace_all(sql, |caps: &regex::Captures<'_>| {
                let number = caps[1].to_string();
                if let Some(value) = lits.get(&number) {
                    return value.clone();
                }
                let next = numbers.len() + 1;
                let slot = *numbers.entry(number.clone()).or_insert(next);
                let kind = if cast {
                    lits.get(&format!("::{number}"))
                } else {
                    None
                };
                kind.map_or_else(|| format!("${slot}"), |kind| format!("${slot}::{kind}"))
            })
            .into_owned()
    }
}

/// Returns the text inside `Some(...)`, or `None`.
fn some_inner(debug: &str) -> Option<&str> {
    debug.strip_prefix("Some(")?.strip_suffix(')')
}

/// Splits `a, [b, c], "d, e"` at its top-level commas.
pub fn split_top(text: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut depth = 0_i64;
    let mut quoted = false;
    let mut escaped = false;
    let mut start = 0;
    for (i, ch) in text.char_indices() {
        if quoted {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                quoted = false;
            }
        } else if ch == '"' {
            quoted = true;
        } else if matches!(ch, '[' | '(' | '{') {
            depth += 1;
        } else if matches!(ch, ']' | ')' | '}') {
            depth -= 1;
        } else if ch == ',' && depth == 0 {
            parts.push(text[start..i].trim().to_string());
            start = i + 1;
        }
    }
    let tail = text[start..].trim();
    if !tail.is_empty() {
        parts.push(tail.to_string());
    }
    parts
}

/// Splits literal maps into types and distinct literal sets.
///
/// The types of a statement rarely change between calls, so one map holds
/// them. Each distinct literal set is kept, up to [`MAX_LITERAL_SETS`]. The
/// first map that names a type wins.
pub fn shape(maps: &[Literals]) -> Entry {
    let mut types: BTreeMap<String, String> = BTreeMap::new();
    let mut sets: BTreeMap<String, Literals> = BTreeMap::new();
    for lits in maps {
        let mut plain = Literals::new();
        for (key, value) in lits {
            if let Some(number) = key.strip_prefix("::") {
                types
                    .entry(number.to_string())
                    .or_insert_with(|| value.clone());
            } else {
                plain.insert(key.clone(), value.clone());
            }
        }
        let text = pyjson::dumps_compact(&literals_value(&plain));
        sets.entry(text).or_insert(plain);
    }
    if types.len() > MAX_TYPED_PARAMS {
        types.clear();
    }
    Entry {
        types,
        literals: sets.into_values().take(MAX_LITERAL_SETS).collect(),
        extra: BTreeMap::new(),
    }
}

fn literals_value(lits: &Literals) -> Value {
    Value::Object(
        lits.iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect(),
    )
}

/// Marks a SQLite statement, so each planner takes only its own backend.
pub fn tagged(mut entry: Entry, sqlite: bool) -> Entry {
    if sqlite {
        entry
            .extra
            .insert("backend".into(), Value::String("sqlite".into()));
    }
    entry
}

/// Returns the parameter maps of an entry: each literal set with its types.
fn entry_maps(entry: &Entry) -> Vec<Literals> {
    entry
        .literals
        .iter()
        .map(|set| {
            let mut map: Literals = entry
                .types
                .iter()
                .map(|(n, t)| (format!("::{n}"), t.clone()))
                .collect();
            map.extend(set.iter().map(|(k, v)| (k.clone(), v.clone())));
            map
        })
        .collect()
}

/// Merges a new census into the committed one.
pub fn merge(old: &Statements, new: &Statements) -> Statements {
    let mut out = old.clone();
    for (catalog, keys) in new {
        for (key, sqls) in keys {
            let slot = out
                .entry(catalog.clone())
                .or_default()
                .entry(key.clone())
                .or_default();
            for (sql, entry) in sqls {
                let prior = slot.get(sql).cloned().unwrap_or_default();
                let mut maps = entry_maps(&prior);
                maps.extend(entry_maps(entry));
                let sqlite = prior.has_backend() || entry.has_backend();
                slot.insert(sql.clone(), tagged(shape(&maps), sqlite));
            }
        }
    }
    out
}

/// Returns the offset of each line start of `source`.
pub fn line_starts(source: &str) -> Vec<usize> {
    let mut starts = vec![0];
    starts.extend(
        source
            .bytes()
            .enumerate()
            .filter(|&(_, b)| b == b'\n')
            .map(|(i, _)| i + 1),
    );
    starts
}

/// Maps `crate/src/path.rs:line` to the profile key of its function.
pub struct Resolver<'a> {
    root: PathBuf,
    catalogs: &'a BTreeMap<String, toml::Table>,
    scanner: &'a Scanner,
    /// The lexed files, keyed by the frame path. `None` marks a missing file.
    pub cache: HashMap<String, Option<(Vec<usize>, Frames)>>,
}

impl<'a> Resolver<'a> {
    /// Returns a resolver over the catalogs, reading source files under `root`.
    pub fn new(
        root: &Path,
        catalogs: &'a BTreeMap<String, toml::Table>,
        scanner: &'a Scanner,
    ) -> Self {
        Self {
            root: root.to_path_buf(),
            catalogs,
            scanner,
            cache: HashMap::new(),
        }
    }

    fn file(&mut self, path: &str) -> Result<Option<&(Vec<usize>, Frames)>> {
        if !self.cache.contains_key(path) {
            let full = self.root.join(path);
            let entry = if full.is_file() {
                let source = fs::read_string(&full)?;
                let (code, _) = lex(&source);
                Some((line_starts(&source), self.scanner.frames_of(&code)))
            } else {
                None
            };
            self.cache.insert(path.to_string(), entry);
        }
        Ok(self.cache.get(path).and_then(Option::as_ref))
    }

    /// Returns `(catalog, key)` for one stack frame, or `None`.
    pub fn key(&mut self, frame: &str) -> Result<Option<(String, String)>> {
        let (path, line) = frame.rsplit_once(':').unwrap_or(("", frame));
        if line.is_empty() || !line.chars().all(|c| c.is_ascii_digit()) {
            return Ok(None);
        }
        let catalogs = self.catalogs;
        let place = catalog_path(path);
        let Some(entry) = self.file(path)? else {
            return Ok(None);
        };
        let Some(catalog) = catalogs.get(&place) else {
            return Ok(None);
        };
        let (starts, frames) = entry;
        let Some(index) = line.parse::<usize>().ok().and_then(|l| l.checked_sub(1)) else {
            return Ok(None);
        };
        let Some(&offset) = starts.get(index) else {
            return Ok(None);
        };
        let mut owner = frames.innermost(offset).and_then(|f| frames.owner(f));
        while let Some(current) = owner {
            let key = frames.chain(current);
            if catalog.contains_key(&key) {
                return Ok(Some((place, key)));
            }
            owner = frames.all[current].parent.and_then(|p| frames.owner(p));
        }
        Ok(None)
    }
}

/// Builds the statements of raw census lines.
///
/// `literals` maps a parameter number to the SQL literal of its bind value.
/// A torn line is counted in `skipped`. Another sample of the same statement
/// usually covers it.
pub fn attribute<'l>(
    lines: impl IntoIterator<Item = &'l str>,
    resolver: &mut Resolver<'_>,
    patterns: &Patterns,
    mut skipped: Option<&mut Vec<String>>,
) -> Result<Statements> {
    type Found = BTreeMap<String, BTreeMap<String, BTreeMap<String, BTreeSet<Literals>>>>;
    let mut found: Found = BTreeMap::new();
    let mut sqlite: BTreeSet<String> = BTreeSet::new();
    for raw in lines {
        let raw = raw.trim();
        if raw.is_empty() {
            continue;
        }
        let Ok(record) = serde_json::from_str::<Value>(raw) else {
            if let Some(list) = skipped.as_deref_mut() {
                list.push(raw.to_string());
            }
            continue;
        };
        let frames = record
            .get("frames")
            .and_then(Value::as_array)
            .with_context(|| format!("census line has no frames list: {raw}"))?;
        for frame in frames {
            let frame = frame
                .as_str()
                .with_context(|| format!("census frame is not a string: {raw}"))?;
            if let Some((catalog, key)) = resolver.key(frame)? {
                let binds = match record.get("binds") {
                    None => "",
                    Some(Value::String(binds)) => binds.as_str(),
                    Some(_) => bail!("census binds are not a string: {raw}"),
                };
                let lits = patterns.literals(binds);
                let sql = record
                    .get("sql")
                    .and_then(Value::as_str)
                    .with_context(|| format!("census line has no sql text: {raw}"))?;
                let sql = patterns.normalize(sql);
                if record.get("backend").and_then(Value::as_str) == Some("sqlite") {
                    sqlite.insert(sql.clone());
                }
                found
                    .entry(catalog)
                    .or_default()
                    .entry(key)
                    .or_default()
                    .entry(sql)
                    .or_default()
                    .insert(lits);
                break;
            }
        }
    }
    Ok(found
        .into_iter()
        .map(|(catalog, keys)| {
            let keys = keys
                .into_iter()
                .map(|(key, sqls)| {
                    let sqls = sqls
                        .into_iter()
                        .map(|(sql, maps)| {
                            let maps: Vec<Literals> = maps.into_iter().collect();
                            let entry = tagged(shape(&maps), sqlite.contains(&sql));
                            (sql, entry)
                        })
                        .collect();
                    (key, sqls)
                })
                .collect();
            (catalog, keys)
        })
        .collect())
}

/// Returns the statements as a JSON value, for [`pyjson`].
pub fn statements_value(statements: &Statements) -> Result<Value> {
    serde_json::to_value(statements).context("encoding the census statements")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_top_keeps_nested_and_quoted_commas() {
        assert_eq!(
            split_top(r#"a, [b, c], "d, e""#),
            vec!["a", "[b, c]", r#""d, e""#]
        );
        assert_eq!(split_top("").len(), 0);
    }

    #[test]
    fn a_state_word_with_a_final_newline_still_counts() -> Result<()> {
        let patterns = Patterns::new()?;
        assert_eq!(
            patterns.literal(r#""RUNNING\n""#),
            Some("'RUNNING\n'".into())
        );
        assert_eq!(patterns.literal(r#""two words""#), None);
        Ok(())
    }

    #[test]
    fn shape_takes_the_first_type_and_sorts_the_sets() {
        let a: Literals = [("::2", "uuid"), ("1", "true")]
            .iter()
            .map(|(k, v)| ((*k).into(), (*v).into()))
            .collect();
        let b = Literals::from([("::2".to_string(), "text".to_string())]);
        let entry = shape(&[a, b]);
        assert_eq!(entry.types.get("2").map(String::as_str), Some("uuid"));
        assert_eq!(entry.literals.len(), 2);
        assert_eq!(entry.literals[1].len(), 0);
    }
}
