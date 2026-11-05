//! `cargo xtask ci coverage`: merge, summarize and ratchet LCOV coverage.
//!
//! This is the port of `.github/ci/coverage.py` (issue #1818).
//!
//! ```text
//! cargo xtask ci coverage convert IN.json --root DIR --output OUT
//! cargo xtask ci coverage merge [--root DIR] --output OUT IN...
//! cargo xtask ci coverage summary IN --json OUT --markdown OUT [--baseline B]
//! cargo xtask ci coverage ratchet --baseline B --summary S [--update]
//! cargo xtask ci coverage --self-test
//! ```
//!
//! The port keeps the bytes that the script wrote. [`pyjson`] prints JSON and
//! numbers as Python prints them. The Python text rules for lines, blanks and
//! integers live in this module. A crash exits with code 1, as an uncaught
//! Python exception did.

use std::collections::BTreeMap;
use std::process::ExitCode;

use smop::prelude::*;

pub mod pyjson;
mod self_test;

use pyjson::Value;

/// The key of one branch side: its line, its block and its side.
pub type BranchKey = (i64, String, String);

/// The line and branch hit counts of one source file.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FileData {
    /// The hits of each line number.
    pub lines: BTreeMap<i64, i128>,
    /// The hits of each branch side.
    pub branches: BTreeMap<BranchKey, i128>,
}

/// The parsed records of a tracefile, by source path.
pub type Records = BTreeMap<String, FileData>;

/// The counts and percentages of one file.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FileRow {
    /// The lines with at least one hit.
    pub lines_hit: usize,
    /// The mapped lines.
    pub lines_total: usize,
    /// The line percentage, or `None` for no lines.
    pub lines_pct: Option<f64>,
    /// The branch sides with at least one hit.
    pub branches_hit: usize,
    /// The branch sides.
    pub branches_total: usize,
    /// The branch percentage, or `None` for no branches.
    pub branches_pct: Option<f64>,
}

/// Arguments of `cargo xtask ci coverage`.
#[derive(Debug, Args)]
pub struct Args {
    /// Run the self-test of the parsers, the merge and the ratchet.
    #[arg(long)]
    pub self_test: bool,
    /// The step to run.
    #[command(subcommand)]
    pub command: Option<Step>,
}

/// Each step of `cargo xtask ci coverage`.
#[derive(Debug, Subcommand)]
pub enum Step {
    /// Convert llvm-cov export JSON into LCOV.
    Convert {
        /// The `llvm-cov export` JSON file.
        input: String,
        /// The workspace root to strip from paths.
        #[arg(long)]
        root: String,
        /// The LCOV file to write.
        #[arg(long)]
        output: String,
    },
    /// Merge LCOV tracefiles.
    Merge {
        /// The workspace root to strip from paths.
        #[arg(long, default_value = "")]
        root: String,
        /// The LCOV file to write.
        #[arg(long)]
        output: String,
        /// The LCOV files to merge.
        #[arg(required = true)]
        inputs: Vec<String>,
    },
    /// Write per-module JSON and Markdown.
    Summary {
        /// The LCOV file to summarize.
        input: String,
        /// The JSON summary to write.
        #[arg(long)]
        json: String,
        /// The Markdown summary to write.
        #[arg(long)]
        markdown: String,
        /// Also render the critical modules against their floors.
        #[arg(long)]
        baseline: Option<String>,
        /// A test job failed; show no verdict.
        #[arg(long)]
        partial: bool,
    },
    /// Compare critical modules with their floors.
    Ratchet {
        /// The committed baseline.
        #[arg(long)]
        baseline: String,
        /// The JSON summary of the run.
        #[arg(long)]
        summary: String,
        /// Raise each floor; never lower one.
        #[arg(long)]
        update: bool,
    },
}

// ── Python text rules ───────────────────────────────────────────────────────

/// True for a character that Python `str.isspace` accepts.
///
/// That is the Unicode `White_Space` set plus the four separators
/// `\x1c` to `\x1f`.
fn py_isspace(c: char) -> bool {
    c.is_whitespace() || ('\x1c'..='\x1f').contains(&c)
}

/// The text of `s.strip()` in Python.
fn py_strip(s: &str) -> &str {
    s.trim_matches(py_isspace)
}

/// The lines of `s.splitlines()` in Python.
fn py_splitlines(s: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut chars = s.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        let breaks = matches!(
            c,
            '\n' | '\r'
                | '\x0b'
                | '\x0c'
                | '\x1c'
                | '\x1d'
                | '\x1e'
                | '\u{85}'
                | '\u{2028}'
                | '\u{2029}'
        );
        if !breaks {
            continue;
        }
        out.push(&s[start..i]);
        let mut end = i + c.len_utf8();
        if c == '\r' && chars.peek().is_some_and(|(_, n)| *n == '\n') {
            chars.next();
            end += 1;
        }
        start = end;
    }
    if start < s.len() {
        out.push(&s[start..]);
    }
    out
}

/// Parses an integer as Python `int(text)` does, for ASCII digits.
///
/// Python allows outer whitespace, a sign, and one underscore between digits.
fn py_int(text: &str) -> Result<i128> {
    let trimmed = py_strip(text);
    let (negative, body) = match trimmed.as_bytes().first() {
        Some(b'-') => (true, &trimmed[1..]),
        Some(b'+') => (false, &trimmed[1..]),
        _ => (false, trimmed),
    };
    let valid = !body.is_empty()
        && !body.starts_with('_')
        && !body.ends_with('_')
        && !body.contains("__")
        && body.bytes().all(|b| b.is_ascii_digit() || b == b'_');
    ensure!(
        valid,
        "ValueError: invalid literal for int() with base 10: {text:?}"
    );
    let digits: String = body.chars().filter(|c| *c != '_').collect();
    let value: i128 = digits
        .parse()
        .with_context(|| format!("ValueError: integer out of range: {text:?}"))?;
    Ok(if negative { -value } else { value })
}

/// Parses a line number.
fn py_line(text: &str) -> Result<i64> {
    i64::try_from(py_int(text)?).context("OverflowError: a line number is out of range")
}

/// Returns `path` relative to `root`, with `/` separators.
fn relative(path: &str, root: &str) -> String {
    let path = path.replace('\\', "/");
    if root.is_empty() {
        return path;
    }
    let prefix = format!("{}/", root.replace('\\', "/").trim_end_matches('/'));
    path.strip_prefix(&prefix)
        .map_or_else(|| path.clone(), str::to_string)
}

// ── Parsers ─────────────────────────────────────────────────────────────────

/// Parses an LCOV tracefile into per-file line and branch hit counts.
///
/// Each file maps to `lines` (line number to hits) and `branches` ((line,
/// block, branch) to hits). LCOV writes `-` for a branch whose block never
/// ran. That counts as zero hits. The parser drops function records, because
/// the report does not use them.
pub fn parse_lcov(text: &str, root: &str) -> Result<Records> {
    let mut records = Records::new();
    let mut current: Option<String> = None;
    for raw in py_splitlines(text) {
        let line = py_strip(raw);
        if let Some(path) = line.strip_prefix("SF:") {
            let key = relative(path, root);
            records.entry(key.clone()).or_default();
            current = Some(key);
        } else if line == "end_of_record" {
            current = None;
        } else if let Some(key) = &current {
            let Some(into) = records.get_mut(key) else {
                continue;
            };
            if let Some(rest) = line.strip_prefix("DA:") {
                let parts: Vec<&str> = rest.split(',').collect();
                ensure!(
                    parts.len() >= 2,
                    "ValueError: not enough values to unpack in {line:?}"
                );
                let number = py_line(parts[0])?;
                let hits = py_int(parts[1])?;
                *into.lines.entry(number).or_insert(0) += hits;
            } else if let Some(rest) = line.strip_prefix("BRDA:") {
                let parts: Vec<&str> = rest.split(',').collect();
                ensure!(
                    parts.len() == 4,
                    "ValueError: a BRDA record needs 4 values: {line:?}"
                );
                let key = (
                    py_line(parts[0])?,
                    parts[1].to_string(),
                    parts[2].to_string(),
                );
                let hits = if parts[3] == "-" {
                    0
                } else {
                    py_int(parts[3])?
                };
                *into.branches.entry(key).or_insert(0) += hits;
            }
        }
    }
    Ok(records)
}

/// The truth value that Python gives a JSON value.
fn truthy(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null => false,
        serde_json::Value::Bool(b) => *b,
        serde_json::Value::Number(n) => n.as_f64().is_some_and(|f| f != 0.0),
        serde_json::Value::String(s) => !s.is_empty(),
        serde_json::Value::Array(a) => !a.is_empty(),
        serde_json::Value::Object(o) => !o.is_empty(),
    }
}

/// Element `index` of a JSON array, or an error like an `IndexError`.
fn at(value: &serde_json::Value, index: usize) -> Result<&serde_json::Value> {
    value
        .as_array()
        .and_then(|a| a.get(index))
        .ok_or_else(|| anyhow!("IndexError: no element {index} in {value}"))
}

/// The value at `key` of a JSON object, or an error like a `KeyError`.
fn key<'a>(value: &'a serde_json::Value, key: &str) -> Result<&'a serde_json::Value> {
    value.get(key).ok_or_else(|| anyhow!("KeyError: {key:?}"))
}

/// A JSON array, or an error.
fn list(value: &serde_json::Value) -> Result<&[serde_json::Value]> {
    value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| anyhow!("TypeError: expected a list, found {value}"))
}

/// A JSON integer, or an error.
fn int(value: &serde_json::Value) -> Result<i128> {
    value
        .as_i64()
        .map(i128::from)
        .or_else(|| value.as_u64().map(i128::from))
        .ok_or_else(|| anyhow!("TypeError: expected an integer count, found {value}"))
}

/// A JSON integer that fits a line number, or an error.
fn line_number(value: &serde_json::Value) -> Result<i64> {
    i64::try_from(int(value)?).context("OverflowError: a line number is out of range")
}

/// The text of `str(value)` in Python, for a JSON scalar.
fn py_text(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) if n.is_f64() => {
            pyjson::float_repr(n.as_f64().unwrap_or(f64::NAN))
        }
        other => other.to_string(),
    }
}

/// One `llvm-cov` segment: line, column, count, and three flags.
struct Segment {
    line: i64,
    count: i128,
    has_count: bool,
    is_entry: bool,
    is_gap: bool,
}

impl Segment {
    fn read(value: &serde_json::Value) -> Result<Self> {
        Ok(Self {
            line: line_number(at(value, 0)?)?,
            count: int(at(value, 2)?)?,
            has_count: truthy(at(value, 3)?),
            is_entry: truthy(at(value, 4)?),
            is_gap: truthy(at(value, 5)?),
        })
    }
}

/// Returns the hit count of each mapped line, as `llvm-cov` computes it.
///
/// This is the rule of `LineCoverageStats` in LLVM. A line is mapped when a
/// counted region starts on it, or when a counted segment wraps into it. A
/// line that starts a skipped region is not mapped, unless a counted region
/// entry also starts on it. A gap region does not count as a start. The
/// count is the largest count that applies.
fn line_counts(segments: &[serde_json::Value]) -> Result<BTreeMap<i64, i128>> {
    let segments: Vec<Segment> = segments.iter().map(Segment::read).collect::<Result<_>>()?;
    let mut out = BTreeMap::new();
    let (Some(first), Some(last)) = (segments.first(), segments.last()) else {
        return Ok(out);
    };
    let mut index = 0;
    let mut wrapped: Option<&Segment> = None;
    let mut current: Vec<&Segment> = Vec::new();
    for line in first.line..=last.line {
        if let Some(seg) = current.last() {
            wrapped = Some(seg);
        }
        current.clear();
        while index < segments.len() && segments[index].line == line {
            current.push(&segments[index]);
            index += 1;
        }
        let starts: Vec<i128> = current
            .iter()
            .filter(|s| s.has_count && s.is_entry && !s.is_gap)
            .map(|s| s.count)
            .collect();
        let skipped = current.first().is_some_and(|s| !s.has_count && s.is_entry);
        let mut mapped = !skipped && (!starts.is_empty() || wrapped.is_some_and(|w| w.has_count));
        // Any counted region entry maps the line, a gap entry too.
        mapped = mapped || current.iter().any(|s| s.has_count && s.is_entry);
        if !mapped {
            continue;
        }
        let carried = wrapped.map_or(0, |w| w.count);
        let count = starts.iter().copied().fold(carried, i128::max);
        out.insert(line, count);
    }
    Ok(out)
}

/// Adds both sides of an `llvm-cov` branch to `into`.
fn add_branch(
    into: &mut FileData,
    line: i64,
    block: &str,
    branch: &serde_json::Value,
) -> Result<()> {
    for (side, index) in [("0", 4), ("1", 5)] {
        let hits = int(at(branch, index)?)?;
        let key = (line, block.to_string(), side.to_string());
        *into.branches.entry(key).or_insert(0) += hits;
    }
    Ok(())
}

/// Converts `llvm-cov export` JSON into the records of [`parse_lcov`].
///
/// The LCOV exporter of LLVM 23 crashes on this crate, so the jobs export
/// JSON. A branch key is its line, its column and its side. Two
/// instantiations of one generic branch share a key, so their hits add up.
/// The branches of each macro expansion count too.
///
/// The JSON does not mark a folded branch side. LLVM drops a folded side
/// from its own totals, and this command counts it as not taken. So a branch
/// percent here can be lower than `cargo llvm-cov report` shows. The
/// difference is the same each night, so the ratchet is not affected.
pub fn from_export(doc: &serde_json::Value, root: &str) -> Result<Records> {
    let kind = doc
        .as_object()
        .ok_or_else(|| anyhow!("AttributeError: the export is not a JSON object"))?
        .get("type");
    ensure!(
        kind.and_then(serde_json::Value::as_str) == Some("llvm.coverage.json.export"),
        "ValueError: not an llvm-cov export JSON file"
    );
    let mut records = Records::new();
    for data in list(key(doc, "data")?)? {
        for item in list(key(data, "files")?)? {
            let filename = key(item, "filename")?;
            let path = relative(
                filename
                    .as_str()
                    .ok_or_else(|| anyhow!("TypeError: a filename is not a string"))?,
                root,
            );
            let into = records.entry(path).or_default();
            for (number, hits) in line_counts(list(key(item, "segments")?)?)? {
                *into.lines.entry(number).or_insert(0) += hits;
            }
            for branch in list(key(item, "branches")?)? {
                let line = line_number(at(branch, 0)?)?;
                add_branch(into, line, &py_text(at(branch, 1)?), branch)?;
            }
            // A branch inside a macro expansion counts at the line of the
            // call, as in the LCOV exporter. Its key also holds the call
            // column, its source file and its own place.
            let no_expansions = serde_json::Value::Array(Vec::new());
            let expansions = item.get("expansions").unwrap_or(&no_expansions);
            for expansion in list(expansions)? {
                add_expansion(into, expansion, root)?;
            }
        }
    }
    Ok(records)
}

/// The name at `id`, when `id < len(names)`, as Python indexes a list.
///
/// A negative id counts from the end, and one past the start fails.
fn name_at(names: &[serde_json::Value], id: i128) -> Result<Option<&serde_json::Value>> {
    let len = i128::try_from(names.len()).unwrap_or(i128::MAX);
    if id >= len {
        return Ok(None);
    }
    let index = if id < 0 { len + id } else { id };
    let index = usize::try_from(index).map_err(|_| anyhow!("IndexError: list index {id}"))?;
    Ok(names.get(index))
}

/// Adds the branches of one macro expansion to `into`.
fn add_expansion(into: &mut FileData, expansion: &serde_json::Value, root: &str) -> Result<()> {
    let region = key(expansion, "source_region")?;
    let line = line_number(at(region, 0)?)?;
    let column = py_text(at(region, 1)?);
    let no_names = serde_json::Value::Array(Vec::new());
    let names = list(expansion.get("filenames").unwrap_or(&no_names))?;
    let no_branches = serde_json::Value::Array(Vec::new());
    let mut seen: BTreeMap<String, i64> = BTreeMap::new();
    for branch in list(expansion.get("branches").unwrap_or(&no_branches))? {
        let file_id = at(branch, 6)?;
        let source = match name_at(names, int(file_id)?)? {
            Some(name) => relative(
                name.as_str()
                    .ok_or_else(|| anyhow!("TypeError: a filename is not a string"))?,
                root,
            ),
            None => py_text(file_id),
        };
        let mut block = format!(
            "{column}@{source}:{}:{}",
            py_text(at(branch, 0)?),
            py_text(at(branch, 1)?)
        );
        // Two nested calls can expand one branch place. The occurrence keeps
        // them apart.
        let count = seen.entry(block.clone()).or_insert(-1);
        *count += 1;
        if *count != 0 {
            block = format!("{block}#{count}");
        }
        add_branch(into, line, &block, branch)?;
    }
    Ok(())
}

// ── Merge and render ────────────────────────────────────────────────────────

/// Sums the hits of several parsed tracefiles.
///
/// The key of a line is its file and number. So a line that two shards run
/// counts once, with the hits of both.
pub fn merge(tracefiles: &[Records]) -> Records {
    let mut out = Records::new();
    for tracefile in tracefiles {
        for (path, data) in tracefile {
            let into = out.entry(path.clone()).or_default();
            for (number, hits) in &data.lines {
                *into.lines.entry(*number).or_insert(0) += hits;
            }
            for (branch, hits) in &data.branches {
                *into.branches.entry(branch.clone()).or_insert(0) += hits;
            }
        }
    }
    out
}

/// Writes parsed records back as an LCOV tracefile.
pub fn render_lcov(records: &Records) -> String {
    let mut out: Vec<String> = Vec::new();
    for (path, data) in records {
        out.push(format!("SF:{path}"));
        for (number, hits) in &data.lines {
            out.push(format!("DA:{number},{hits}"));
        }
        for ((number, block, branch), hits) in &data.branches {
            out.push(format!("BRDA:{number},{block},{branch},{hits}"));
        }
        let branches = &data.branches;
        out.push(format!("BRF:{}", branches.len()));
        out.push(format!(
            "BRH:{}",
            branches.values().filter(|h| **h > 0).count()
        ));
        out.push(format!("LF:{}", data.lines.len()));
        out.push(format!(
            "LH:{}",
            data.lines.values().filter(|h| **h > 0).count()
        ));
        out.push("end_of_record".to_string());
    }
    out.join("\n") + "\n"
}

/// The percentage `hit / total`, rounded to two places, or `None`.
#[allow(clippy::cast_precision_loss)]
fn pct(hit: usize, total: usize) -> Option<f64> {
    (total != 0).then(|| pyjson::py_round(100.0 * hit as f64 / total as f64, 2))
}

/// Returns line and branch counts and percentages for each file.
pub fn summarize(records: &Records) -> BTreeMap<String, FileRow> {
    records
        .iter()
        .map(|(path, data)| {
            let lines_hit = data.lines.values().filter(|h| **h > 0).count();
            let branches_hit = data.branches.values().filter(|h| **h > 0).count();
            let row = FileRow {
                lines_hit,
                lines_total: data.lines.len(),
                lines_pct: pct(lines_hit, data.lines.len()),
                branches_hit,
                branches_total: data.branches.len(),
                branches_pct: pct(branches_hit, data.branches.len()),
            };
            (path.clone(), row)
        })
        .collect()
}

/// A count as a JSON integer.
fn count_value(n: usize) -> Value {
    Value::Int(i128::try_from(n).unwrap_or(i128::MAX))
}

/// A percentage as a JSON float, or `null`.
fn pct_value(p: Option<f64>) -> Value {
    p.map_or(Value::Null, Value::Float)
}

/// The summary document: `{"files": {path: row}}`.
pub fn summary_value(files: &BTreeMap<String, FileRow>) -> Value {
    let rows = files
        .iter()
        .map(|(path, row)| {
            let fields = vec![
                ("lines_hit".to_string(), count_value(row.lines_hit)),
                ("lines_total".to_string(), count_value(row.lines_total)),
                ("lines_pct".to_string(), pct_value(row.lines_pct)),
                ("branches_hit".to_string(), count_value(row.branches_hit)),
                (
                    "branches_total".to_string(),
                    count_value(row.branches_total),
                ),
                ("branches_pct".to_string(), pct_value(row.branches_pct)),
            ];
            (path.clone(), Value::Object(fields))
        })
        .collect();
    Value::Object(vec![("files".to_string(), Value::Object(rows))])
}

// ── The ratchet ─────────────────────────────────────────────────────────────

/// The value at `key` of an object, like `dict.get`. A non-object fails.
fn get<'a>(value: &'a Value, key: &str) -> Result<Option<&'a Value>> {
    match value {
        Value::Object(_) => Ok(value.get(key)),
        _ => bail!("AttributeError: cannot read {key:?} of a non-object"),
    }
}

/// The value at `key`, with `null` read as missing.
fn get_some<'a>(value: &'a Value, key: &str) -> Result<Option<&'a Value>> {
    Ok(get(value, key)?.filter(|v| !v.is_null()))
}

/// The row of `path` in the summary, or `None` when it is falsy.
fn summary_row<'a>(summary: &'a Value, path: &str) -> Result<Option<&'a Value>> {
    Ok(get(summary.index("files")?, path)?.filter(|row| row.truthy()))
}

/// Returns one message for each critical module below its floor.
///
/// A module fails when its coverage is lower than its floor minus the
/// tolerance. A module `tolerance` overrides the global one. A small module
/// needs it, because one branch there is worth more than a point. A module
/// with no data fails too. Otherwise a moved or renamed module would leave
/// the gate in silence. A `null` floor is not recorded yet, so it passes. The
/// guard forbids a `null` floor in the committed baseline.
pub fn check(baseline: &Value, summary: &Value) -> Result<Vec<String>> {
    let mut found = Vec::new();
    for (path, floors) in baseline.index("modules")?.items()? {
        let global = baseline.index("tolerance")?;
        let tolerance = get(floors, "tolerance")?.unwrap_or(global);
        let Some(row) = summary_row(summary, path)? else {
            found.push(format!("{path}: no coverage data"));
            continue;
        };
        if !row.index("lines_total")?.truthy() {
            found.push(format!("{path}: no coverage data"));
            continue;
        }
        for kind in ["lines", "branches"] {
            let floor = get_some(floors, kind)?;
            let current = row.index(&format!("{kind}_pct"))?;
            let Some(floor) = floor else {
                continue;
            };
            if current.is_null() {
                found.push(format!(
                    "{path}: no branch data, but the {kind} floor is {}",
                    floor.py_str()
                ));
            } else if current.number()? < pyjson::py_round(floor.number()? - tolerance.number()?, 2)
            {
                found.push(format!(
                    "{path}: {kind} coverage {}% is below the floor {}% minus the tolerance {}",
                    current.py_str(),
                    floor.py_str(),
                    tolerance.py_str()
                ));
            }
        }
    }
    Ok(found)
}

/// Rounds a percentage down to two places.
///
/// A fused multiply-add would round once where Python rounds twice. So the
/// result could differ from the script in the last bit.
#[allow(clippy::suboptimal_flops)]
fn round_down(pct: f64) -> f64 {
    // The epsilon stops float error from taking 37.80 down to 37.79.
    (pct * 100.0 + 1e-6).floor() / 100.0
}

/// Returns a copy of `baseline` with each floor raised to the current value.
///
/// A floor never drops. A `null` floor takes the current value. The flag is
/// true when some floor rose.
pub fn raised(baseline: &Value, summary: &Value) -> Result<(Value, bool)> {
    let mut out = baseline.clone();
    let mut changed = false;
    let modules = match &mut out {
        Value::Object(items) => items
            .iter_mut()
            .find(|(k, _)| k == "modules")
            .map(|(_, v)| v)
            .ok_or_else(|| anyhow!("KeyError: \"modules\""))?,
        _ => bail!("TypeError: the baseline is not an object"),
    };
    for (path, floors) in modules.items_mut()? {
        let row = summary_row(summary, path)?;
        for kind in ["lines", "branches"] {
            let current = match row {
                Some(row) => get_some(row, &format!("{kind}_pct"))?,
                None => None,
            };
            let Some(current) = current else {
                continue;
            };
            let current = round_down(current.number()?);
            let rises = match get_some(floors, kind)? {
                None => true,
                Some(floor) => current > floor.number()?,
            };
            if rises {
                floors.set(kind, Value::Float(current))?;
                changed = true;
            }
        }
    }
    Ok((out, changed))
}

/// A percentage as the report prints it, or `n/a`.
fn fmt_pct(pct: Option<f64>) -> String {
    pct.map_or_else(|| "n/a".to_string(), |p| format!("{p:.2}%"))
}

/// A JSON percentage as the report prints it.
fn fmt_value(value: Option<&Value>) -> Result<String> {
    match value.filter(|v| !v.is_null()) {
        None => Ok("n/a".to_string()),
        Some(v) => Ok(fmt_pct(Some(v.number()?))),
    }
}

/// The crate of a source path: the part before `/src/`, or its first part.
fn crate_of(path: &str) -> &str {
    if path.contains("/src/") {
        path.split("/src/").next().unwrap_or(path)
    } else if path.starts_with("src/") {
        "."
    } else {
        path.split('/').next().unwrap_or(path)
    }
}

/// Renders the per-crate totals and the per-file table.
pub fn files_markdown(files: &BTreeMap<String, FileRow>) -> String {
    let mut crates: BTreeMap<&str, [usize; 4]> = BTreeMap::new();
    for (path, row) in files {
        let total = crates.entry(crate_of(path)).or_insert([0; 4]);
        total[0] += row.lines_hit;
        total[1] += row.lines_total;
        total[2] += row.branches_hit;
        total[3] += row.branches_total;
    }
    let mut out: Vec<String> = [
        "## Coverage by crate",
        "",
        "| Crate | Lines | Branches |",
        "|---|---:|---:|",
    ]
    .iter()
    .map(|s| (*s).to_string())
    .collect();
    for (name, t) in &crates {
        out.push(format!(
            "| `{name}` | {} | {} |",
            fmt_pct(pct(t[0], t[1])),
            fmt_pct(pct(t[2], t[3]))
        ));
    }
    for line in [
        "",
        "<details><summary>Coverage by module</summary>",
        "",
        "| Module | Lines | Branches |",
        "|---|---:|---:|",
    ] {
        out.push(line.to_string());
    }
    for (path, row) in files {
        let lines = format!(
            "{} ({}/{})",
            fmt_pct(row.lines_pct),
            row.lines_hit,
            row.lines_total
        );
        let branches = format!(
            "{} ({}/{})",
            fmt_pct(row.branches_pct),
            row.branches_hit,
            row.branches_total
        );
        out.push(format!("| `{path}` | {lines} | {branches} |"));
    }
    for line in ["", "</details>", ""] {
        out.push(line.to_string());
    }
    out.join("\n")
}

/// The command that raises the floors, as the summary tells it.
const RAISE_COMMAND: &str = "cargo xtask ci coverage ratchet --update --baseline \
                             .github/ci/coverage-baseline.json --summary summary.json";

/// Renders each critical module against its floor.
///
/// A `partial` run had a failed test job. Its coverage is short, so the
/// summary shows no verdict and suggests no new floor.
pub fn ratchet_markdown(
    baseline: &Value,
    summary: &Value,
    found: &[String],
    partial: bool,
) -> Result<String> {
    let mut out: Vec<String> = vec!["## Critical modules (issue #1818)".into(), String::new()];
    out.push(format!(
        "Tolerance: {} points, unless a module sets its own.",
        baseline.index("tolerance")?.py_str()
    ));
    out.push(String::new());
    out.push("| Module | Lines | Floor | Branches | Floor |".into());
    out.push("|---|---:|---:|---:|---:|".into());
    let modules = baseline.index("modules")?.items()?;
    for (path, floors) in modules {
        let row = summary_row(summary, path)?;
        let current = |kind: &str| -> Result<String> {
            match row {
                Some(row) => fmt_value(get(row, kind)?),
                None => Ok("n/a".to_string()),
            }
        };
        out.push(format!(
            "| `{path}` | {} | {} | {} | {} |",
            current("lines_pct")?,
            fmt_value(get(floors, "lines")?)?,
            current("branches_pct")?,
            fmt_value(get(floors, "branches")?)?
        ));
    }
    out.push(String::new());
    if partial {
        out.push(
            "Ratchet skipped: a test job failed, so the coverage of this run is partial.".into(),
        );
        out.push(String::new());
        return Ok(out.join("\n"));
    }
    let mut unarmed = true;
    for (_, floors) in modules {
        for kind in ["lines", "branches"] {
            if get_some(floors, kind)?.is_some() {
                unarmed = false;
            }
        }
    }
    if unarmed {
        out.push(
            "No floor is recorded yet. The ratchet checks only that each module has data.".into(),
        );
        out.push(String::new());
    }
    if found.is_empty() {
        out.push("No critical module is below its floor.".into());
    } else {
        out.extend(found.iter().map(|m| format!("- FAIL: {m}")));
    }
    if raised(baseline, summary)?.1 {
        out.push(String::new());
        out.push(format!(
            "A floor can rise. Download `summary.json`, run `{RAISE_COMMAND}`, and commit the baseline."
        ));
    }
    out.push(String::new());
    Ok(out.join("\n"))
}

// ── The steps ───────────────────────────────────────────────────────────────

/// Reads a UTF-8 text file.
fn read_text(path: &str) -> Result<String> {
    std::fs::read_to_string(path).with_context(|| format!("reading {path}"))
}

/// Writes a text file.
fn write_text(path: &str, text: &str) -> Result<()> {
    std::fs::write(path, text).with_context(|| format!("writing {path}"))
}

/// Reads a JSON file in key order.
fn read_json(path: &str) -> Result<Value> {
    pyjson::loads(&read_text(path)?).with_context(|| format!("parsing {path}"))
}

/// Runs one step and returns its exit code.
fn run_step(step: Step) -> Result<ExitCode> {
    match step {
        Step::Convert {
            input,
            root,
            output,
        } => {
            let doc: serde_json::Value = serde_json::from_str(&read_text(&input)?)
                .with_context(|| format!("parsing {input}"))?;
            write_text(&output, &render_lcov(&from_export(&doc, &root)?))?;
        }
        Step::Merge {
            root,
            output,
            inputs,
        } => {
            let tracefiles = inputs
                .iter()
                .map(|p| parse_lcov(&read_text(p)?, &root))
                .collect::<Result<Vec<_>>>()?;
            write_text(&output, &render_lcov(&merge(&tracefiles)))?;
        }
        Step::Summary {
            input,
            json,
            markdown,
            baseline,
            partial,
        } => {
            let files = summarize(&merge(&[parse_lcov(&read_text(&input)?, "")?]));
            let summary = summary_value(&files);
            write_text(&json, &(pyjson::dumps_indent2(&summary) + "\n"))?;
            let mut text = files_markdown(&files);
            if let Some(baseline) = baseline.filter(|b| !b.is_empty()) {
                let baseline = read_json(&baseline)?;
                let found = check(&baseline, &summary)?;
                text = ratchet_markdown(&baseline, &summary, &found, partial)? + "\n" + &text;
            }
            write_text(&markdown, &text)?;
        }
        Step::Ratchet {
            baseline: baseline_path,
            summary,
            update,
        } => {
            let baseline = read_json(&baseline_path)?;
            let summary = read_json(&summary)?;
            if update {
                let (new, _) = raised(&baseline, &summary)?;
                write_text(&baseline_path, &(pyjson::dumps_indent2(&new) + "\n"))?;
                println!("updated {baseline_path}");
                return Ok(ExitCode::SUCCESS);
            }
            let found = check(&baseline, &summary)?;
            for message in &found {
                println!("FAIL: {message}");
            }
            if found.is_empty() {
                println!("No critical module is below its floor.");
            }
            return Ok(crate::util::gate_exit(found.is_empty()));
        }
    }
    Ok(ExitCode::SUCCESS)
}

/// Runs `cargo xtask ci coverage`.
///
/// Each `run` of the command groups returns a `Result`, so this one does too.
#[allow(clippy::unnecessary_wraps)]
pub fn run(args: Args) -> Result<ExitCode> {
    let outcome = match (args.self_test, args.command) {
        (true, None) => self_test::run().map(|()| {
            println!("coverage self-test: ok");
            ExitCode::SUCCESS
        }),
        (true, Some(_)) => {
            eprintln!("error: --self-test takes no step");
            return Ok(ExitCode::from(2));
        }
        (false, None) => {
            eprintln!(
                "error: the following arguments are required: <convert|merge|summary|ratchet>"
            );
            return Ok(ExitCode::from(2));
        }
        (false, Some(step)) => run_step(step),
    };
    // An uncaught Python exception exited with code 1, so a crash does too.
    Ok(outcome.unwrap_or_else(|error| {
        eprintln!("error: {error:#}");
        ExitCode::FAILURE
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_self_test_passes() {
        self_test::run().unwrap();
    }

    #[test]
    fn splitlines_and_strip_follow_python() {
        assert_eq!(
            py_splitlines("a\r\nb\rc\nd\x0be"),
            vec!["a", "b", "c", "d", "e"]
        );
        assert_eq!(py_splitlines("a\n\nb\n"), vec!["a", "", "b"]);
        assert_eq!(py_splitlines(""), Vec::<&str>::new());
        assert_eq!(py_strip("\x1c DA:1,2 \u{a0}"), "DA:1,2");
    }

    #[test]
    fn py_int_accepts_what_python_accepts() {
        assert_eq!(py_int(" 12 ").ok(), Some(12));
        assert_eq!(py_int("+3").ok(), Some(3));
        assert_eq!(py_int("-1_000").ok(), Some(-1000));
        for bad in ["", "1.0", "_1", "1__0", "x", "-"] {
            assert!(py_int(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn relative_strips_only_a_whole_root_prefix() {
        assert_eq!(relative("/w/r/a.rs", "/w/r"), "a.rs");
        assert_eq!(relative("/w/r/a.rs", "/w/r/"), "a.rs");
        assert_eq!(relative("/w/rx/a.rs", "/w/r"), "/w/rx/a.rs");
        assert_eq!(relative("C:\\w\\a.rs", "C:\\w"), "a.rs");
        assert_eq!(relative("/w/a.rs", ""), "/w/a.rs");
    }

    #[test]
    fn a_short_brda_record_fails_the_parse() {
        let text = "SF:a.rs\nBRDA:1,0,0\nend_of_record\n";
        assert!(parse_lcov(text, "").is_err());
    }

    #[test]
    fn render_of_no_records_is_one_newline() {
        assert_eq!(render_lcov(&Records::new()), "\n");
    }

    #[test]
    fn crate_of_reads_the_source_layout() {
        assert_eq!(crate_of("autumn-harvest/src/queue.rs"), "autumn-harvest");
        assert_eq!(crate_of("src/lib.rs"), ".");
        assert_eq!(crate_of("tests/a.rs"), "tests");
    }
}
