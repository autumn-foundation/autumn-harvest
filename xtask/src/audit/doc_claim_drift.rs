//! `cargo xtask audit doc-claim-drift`: fail when a doc claim contradicts the shipped code (issue #1831).
//!
//! This is the port of `docs/audits/doc-claim-drift.py`. It has four rules.
//!
//! 1. event-row-count. The `event.rs` row in `docs/architecture.md` states no
//!    `WorkflowEvent` variant count. A hard-coded count drifts: two rows once
//!    said 41 and 35 while the enum had 50.
//! 2. duplicate-row. No two rows of one table in `docs/architecture.md` share
//!    a first cell.
//! 3. release-notes. If `RELEASE_NOTES.md` has a version heading, its newest
//!    version is the workspace version.
//! 4. stale-claim. Each `STALE_CLAIMS` pin stays absent from its file. A pin is
//!    the exact wording that a shipped change made false.
//!
//! Each rule checks a fixed place or an exact phrase. No rule judges free
//! prose. A regex cannot tell a total from a subset with certainty. Nor can it
//! tell harvest from a competitor.
//!
//! # Usage
//!
//! ```sh
//! cargo xtask audit doc-claim-drift
//! cargo xtask audit doc-claim-drift --self-test
//! ```

use std::path::Path;
use std::process::ExitCode;

use regex::Regex;
use smop::prelude::*;

use super::corpus_link_check::pycompat;
use crate::util;

/// Arguments of `cargo xtask audit doc-claim-drift`.
#[derive(Debug, Args)]
pub struct Args {
    /// Run the built-in fixtures instead of the repository check.
    #[arg(long)]
    pub self_test: bool,
}

/// The path of this module, for the messages that ask for an update here.
const THIS_MODULE: &str = "xtask/src/audit/doc_claim_drift.rs";

/// The pins, as `(path, pattern, the fact that makes the claim false)`.
///
/// Each pattern pins the exact wording that a shipped change made false, as a
/// regression check. A pattern does not try to judge new prose. A general
/// phrase such as "no cross-region failover" is true of some competitors, and
/// of running workflows. `\s+` lets a pin match across a line wrap. The
/// patterns use Python regex syntax, which `pycompat::regex` compiles.
pub const STALE_CLAIMS: &[(&str, &str, &str)] = &[
    (
        "docs/comparison.md",
        r"\*\*Planned:\*\*\s+cross-region\s+DR|Planned\s+R&D:\s+\[#954\]",
        "cross-region DR shipped (issue #954), see docs/cross-region-dr.md",
    ),
    (
        "docs/comparison.md",
        concat!(
            r"no\s+built-in\s+multi-region\s+replication\s+or\s+failover\s+today",
            r"|Single-region\s+—\s+no\s+multi-region\s+DR\s+or\s+replication"
        ),
        "cross-region DR shipped (issue #954), see docs/cross-region-dr.md",
    ),
    (
        "docs/comparison.md",
        concat!(
            r"\*\*No\s+cross-shard\s+workflows\.\*\*",
            r"|cross-shard\s+workflows\s+are\s+(explicitly\s+)?out\s+of\s+scope"
        ),
        "cross-shard child placement shipped (issue #956)",
    ),
    (
        "docs/architecture.md",
        r"Cross-shard\s+rebalancing\s+of\s+existing\s+workflows\s+is\s+out\s+of\s+scope\.",
        "shard rebalancing shipped (issue #964), see shard_rebalance.rs",
    ),
    (
        "docs/sharding.md",
        r"\(cross-shard\s+rebalancing\s+is\s+not\s+supported\)",
        "shard rebalancing shipped (issue #964), see shard_rebalance.rs",
    ),
    (
        "docs/architecture.md",
        r"cross-shard\s+child\s+fan-out\s+is\s+out\s+of\s+scope",
        "the `_placed` fan-out variants take a ChildPlacement (issue #956)",
    ),
    (
        "docs/sharding.md",
        r"geo-replication\s*/\s*cross-region\s+failover",
        "cross-region DR shipped (issue #954), see docs/cross-region-dr.md",
    ),
    (
        "docs/autumn-workflow-architecture.md",
        r"(?i)harvest_task_queue`?[^\n.]*list[- ]partition",
        "harvest_task_queue is one table, not partitioned (issue #1811)",
    ),
    (
        "autumn-harvest-redis/src/lib.rs",
        r"\A//! Redis Streams task queue adapter",
        "the worker uses the crate as a dispatch channel for task references",
    ),
    (
        "docs/vantage-ui.md",
        r"Terminate\*\*\s+—\s+Disabled\s+button",
        "the detail page posts to /workflows/{exec_id}/terminate (issue #788)",
    ),
    (
        "docs/comparison.md",
        r"\*\*Planned:\*\*\s+a\s+TypeScript\s+activity-worker\s+SDK",
        "#959 and #955 closed as not planned; ADR 0002 rules out worker SDKs",
    ),
    (
        "docs/comparison.md",
        r"DAG\s+\*\*graph\*\*\s+visualization[^.]*still\s+Phase\s+4",
        "the DAG detail page renders an inline SVG run graph (issue #690)",
    ),
    (
        "skills/SKILL.md",
        r"\*\*Version\*\*:\s+0\.[0-6]\.",
        "the skill tracks the workspace version",
    ),
    (
        "autumn-harvest/src/worker.rs",
        r"TODO\(#606 step 9\)",
        "#606 step 9 is wired: build_activity_enqueue_plan pins session tasks",
    ),
    (
        "docs/comparison.md",
        r"No\s+cell\s+on\s+this\s+page\s+claims\s+a\s+throughput\s+or\s+latency\s+comparison",
        "assay #14 reran the comparison on 0.7.0 (issue #1972)",
    ),
    (
        "README.md",
        r"The\s+SQLite\s+backend\s+does\s+not\.",
        "the SQLite backend enforces both history caps (issue #2023)",
    ),
    (
        "autumn-harvest-sqlite/README.md",
        r"search\s+attributes,\s+and\s+`continue_as_new`",
        "the SQLite backend supports continue_as_new (issue #2023)",
    ),
    (
        "docs/rnd/sqlite-feasibility.md",
        r"local\s+and\s+external\s+activities,\s+cancellable\s+timers",
        "the SQLite backend runs local activities and cancellable timers (issue #2029)",
    ),
    (
        "autumn-harvest-sqlite/README.md",
        r"external\s+signals/cancels,\s+local\s+activities",
        "the SQLite backend runs local activities (issue #2029)",
    ),
];

/// The compiled patterns of the audit.
struct Patterns {
    /// The module cell names `event.rs`, perhaps with a path, emphasis or a link.
    event_row_cell: Regex,
    /// A count beside "variant", in either order, with only separators between.
    ///
    /// It matches "41 variants", "49 Variants", "Variants: 49" and
    /// "variants — 1,050".
    count: Regex,
    /// A version heading. The version is a semver core with an optional
    /// prerelease and build part. Heading punctuation after it, as in
    /// "## 0.7.0: notes", is not part of it.
    version_heading: Regex,
    /// A line that ends a paragraph: blank, or the start of an HTML comment
    /// block, a heading or a fence.
    paragraph_end: Regex,
    delimiter_cell: Regex,
    link: Regex,
    marks: Regex,
}

impl Patterns {
    fn new() -> Result<Self> {
        let semver_id = r"[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*";
        Ok(Self {
            event_row_cell: pycompat::regex(r"(?:^|/)event\.rs$")?,
            count: pycompat::regex(
                r"(?i)\b\d[\d,]*[\s:=—-]*variants?\b|\bvariants?\b[\s:=(—-]*\d",
            )?,
            version_heading: pycompat::regex(&format!(
                r"(?m)^ {{0,3}}##\s+\[?v?(\d+\.\d+\.\d+(?:-{semver_id})?(?:\+{semver_id})?)"
            ))?,
            paragraph_end: pycompat::regex(r"^(?:\s*$| {0,3}(?:<!--|#{1,6}(?:\s|$)|`{3,}|~{3,}))")?,
            delimiter_cell: pycompat::regex(r"^:?-+:?$")?,
            link: pycompat::regex(r"!?\[([^\]]*)\]\([^)]*\)")?,
            marks: pycompat::regex(r"[`*_~]")?,
        })
    }
}

/// Returns the 1-based line of a byte offset.
fn line_of(text: &str, offset: usize) -> usize {
    text[..offset].matches('\n').count() + 1
}

/// Splits a line that fully matches a `CommonMark` fence: up to 3 spaces, then
/// 3 or more backticks or tildes.
///
/// The result is the delimiter run and the rest of the line.
fn fence_marker(line: &str) -> Option<(&str, &str)> {
    let indent = line.len() - line.trim_start_matches(' ').len();
    if indent > 3 {
        return None;
    }
    let rest = &line[indent..];
    let c = rest.chars().next().filter(|c| *c == '`' || *c == '~')?;
    let run = rest.len() - rest.trim_start_matches(c).len();
    (run >= 3).then(|| (&rest[..run], &rest[run..]))
}

/// Finds the first whole run of exactly `run` backticks that starts at or after `from`.
///
/// A run that starts before `from` is not whole from there, so it never counts.
fn find_closer(chars: &[char], run: usize, from: usize) -> Option<(usize, usize)> {
    let mut p = from;
    while p < chars.len() {
        if chars[p] != '`' || (p > 0 && chars[p - 1] == '`') {
            p += 1;
            continue;
        }
        let mut end = p;
        while end < chars.len() && chars[end] == '`' {
            end += 1;
        }
        if end - p == run {
            return Some((p, end));
        }
        p = end;
    }
    None
}

/// Finds the first backtick run that starts at or after `from`.
fn find_tick_run(chars: &[char], from: usize) -> Option<(usize, usize)> {
    let start = (from..chars.len()).find(|&p| chars[p] == '`')?;
    let mut end = start;
    while end < chars.len() && chars[end] == '`' {
        end += 1;
    }
    Some((start, end))
}

/// Blanks code-span text, so its delimiters cannot open a comment.
///
/// A code span can continue onto the next line of a paragraph. `open_run` is
/// the backtick run of a span still open from the last line, or 0. `rest` is
/// the remaining text of the paragraph. A run with no closer there is a
/// literal backtick, as in `CommonMark`. The result is the masked line and the
/// run still open at its end.
fn mask_code_spans(line: &str, mut open_run: usize, rest: &str) -> (Vec<char>, usize) {
    let chars: Vec<char> = line.chars().collect();
    let rest: Vec<char> = rest.chars().collect();
    let mut out = chars.clone();
    let mut i = 0;
    while i < chars.len() {
        if open_run > 0 {
            let close = find_closer(&chars, open_run, i);
            let end = close.map_or(chars.len(), |c| c.1);
            out[i..end].fill(' ');
            if close.is_none() {
                break;
            }
            i = end;
            open_run = 0;
        } else {
            let Some((start, end)) = find_tick_run(&chars, i) else {
                break;
            };
            let run = end - start;
            if find_closer(&chars, run, end).is_none() && find_closer(&rest, run, 0).is_none() {
                // No closer in the paragraph: a literal backtick run.
                i = end;
                continue;
            }
            out[start..end].fill(' ');
            i = end;
            open_run = run;
        }
    }
    (out, open_run)
}

/// Finds `needle` in `hay` at or after `from`, by character index.
fn find_chars(hay: &[char], needle: &str, from: usize) -> Option<usize> {
    let needle: Vec<char> = needle.chars().collect();
    (from..hay.len()).find(|&i| hay[i..].starts_with(&needle))
}

/// Returns the visible part of a line, and whether a comment stays open.
///
/// The delimiters are found in `masked`, a copy with code spans blanked. So a
/// `<!--` in a code span is text. The visible text comes from `line`.
fn strip_html_comments(line: &str, masked: &[char], mut in_comment: bool) -> (String, bool) {
    let chars: Vec<char> = line.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        if in_comment {
            let Some(end) = find_chars(masked, "-->", i) else {
                return (out, true);
            };
            i = end + 3;
            in_comment = false;
        } else {
            let Some(start) = find_chars(masked, "<!--", i) else {
                out.extend(&chars[i..]);
                break;
            };
            out.extend(&chars[i..start]);
            i = start + 4;
            in_comment = true;
        }
    }
    (out, in_comment)
}

/// Returns `(line number, line)` for the rendered text outside fenced blocks.
///
/// A fence closes only on a run of the same character, as in `CommonMark`.
/// The run is at least as long as the opener, with nothing after it. So a
/// four-backtick fence can show a three-backtick block inside it. HTML
/// comment text is not rendered, so it is blanked. A line keeps its number.
fn markdown_lines(pats: &Patterns, text: &str) -> Vec<(usize, String)> {
    let lines = pycompat::splitlines(text);
    let mut found = Vec::new();
    let mut fence: Option<String> = None;
    let mut in_comment = false;
    let mut open_run = 0;
    for (idx, line) in lines.iter().enumerate() {
        let n = idx + 1;
        let marker = fence_marker(line);
        if let Some(open) = &fence {
            if let Some((delim, rest)) = marker
                && delim.chars().next() == open.chars().next()
                && delim.len() >= open.len()
                && pycompat::strip(rest).is_empty()
            {
                fence = None;
            }
            continue;
        }
        if pycompat::strip(line).is_empty() {
            // A code span cannot cross a blank line.
            open_run = 0;
        }
        let is_opener =
            marker.is_some_and(|(delim, rest)| !(delim.starts_with('`') && rest.contains('`')));
        if is_opener && !in_comment && open_run == 0 {
            fence = marker.map(|(delim, _)| delim.to_owned());
            continue;
        }
        let rest: Vec<&str> = lines[n..]
            .iter()
            .take_while(|later| !pats.paragraph_end.is_match(later))
            .copied()
            .collect();
        let (masked, run) = mask_code_spans(line, open_run, &rest.join("\n"));
        open_run = run;
        let (visible, still_open) = strip_html_comments(line, &masked, in_comment);
        in_comment = still_open;
        found.push((n, visible));
    }
    found
}

/// Splits on each pipe that no backslash escapes.
fn split_cells(body: &str) -> Vec<&str> {
    let mut cells = Vec::new();
    let mut start = 0;
    let mut prev = None;
    for (i, c) in body.char_indices() {
        if c == '|' && prev != Some('\\') {
            cells.push(&body[start..i]);
            start = i + 1;
        }
        prev = Some(c);
    }
    cells.push(&body[start..]);
    cells
}

/// Splits a GFM table line into cells, or returns `None` if it has no pipe.
///
/// Up to 3 spaces of indent are allowed, and the outer pipes are optional.
fn table_cells(line: &str) -> Option<Vec<String>> {
    let indent = line.len() - line.trim_start_matches(' ').len();
    if indent > 3 || split_cells(line).len() < 2 {
        return None;
    }
    let mut body = pycompat::strip(line);
    if let Some(inner) = body.strip_prefix('|') {
        body = inner;
    }
    if body.ends_with('|') && !body.ends_with("\\|") {
        body = &body[..body.len() - 1];
    }
    Some(
        split_cells(body)
            .into_iter()
            .map(|c| pycompat::strip(c).to_owned())
            .collect(),
    )
}

/// The body rows of one table, as `(line number, cells)`.
type Table = Vec<(usize, Vec<String>)>;

/// Returns each GFM table as its body rows.
///
/// A table is a header row, then a delimiter row such as `|---|:--:|`, then
/// the body rows up to the first line without a pipe. Fenced blocks are
/// skipped, and a fence ends a table.
fn markdown_tables(pats: &Patterns, text: &str) -> Vec<Table> {
    let mut tables = Vec::new();
    let mut prev: Option<(usize, Option<Vec<String>>)> = None;
    let mut table: Option<Table> = None;
    for (n, line) in markdown_lines(pats, text) {
        let cells = table_cells(&line);
        let contiguous = prev.as_ref().is_some_and(|p| n == p.0 + 1);
        match (&mut table, &cells) {
            (Some(rows), Some(c)) if contiguous => rows.push((n, c.clone())),
            _ => {
                if let Some(rows) = table.take()
                    && !rows.is_empty()
                {
                    tables.push(rows);
                }
                let is_delimiter = cells
                    .as_ref()
                    .is_some_and(|c| c.iter().all(|cell| pats.delimiter_cell.is_match(cell)));
                let prev_has_cells = prev.as_ref().is_some_and(|p| p.1.is_some());
                if is_delimiter && contiguous && prev_has_cells {
                    table = Some(Vec::new());
                }
            }
        }
        prev = Some((n, cells));
    }
    if let Some(rows) = table
        && !rows.is_empty()
    {
        tables.push(rows);
    }
    tables
}

/// Returns text without link syntax, emphasis or code marks.
fn plain_markdown(pats: &Patterns, text: &str) -> String {
    let unlinked = pats.link.replace_all(text, "${1}");
    pycompat::strip(&pats.marks.replace_all(&unlinked, "")).to_owned()
}

/// Returns a message for each variant count in an `event.rs` row.
fn event_row_count_findings(pats: &Patterns, page: &str, text: &str) -> Vec<String> {
    let mut found = Vec::new();
    for table in markdown_tables(pats, text) {
        for (n, cells) in table {
            if !pats
                .event_row_cell
                .is_match(&plain_markdown(pats, &cells[0]))
            {
                continue;
            }
            // Emphasis and code marks do not change a rendered count.
            let plain = plain_markdown(pats, &cells.join(" | "));
            for m in pats.count.find_iter(&plain) {
                found.push(format!(
                    "{page}:{n}: the `event.rs` row states \"{}\". Drop the count: it drifts from event.rs.",
                    m.as_str()
                ));
            }
        }
    }
    found
}

/// Returns a message for each repeated first cell within one table body.
fn duplicate_row_findings(pats: &Patterns, page: &str, text: &str) -> Vec<String> {
    let mut found = Vec::new();
    for table in markdown_tables(pats, text) {
        let mut seen: Vec<(String, usize)> = Vec::new();
        for (n, cells) in table {
            // Compare what a reader sees: `a.rs` and **a.rs** are one name.
            let cell = plain_markdown(pats, &cells[0]);
            if cell.is_empty() {
                continue;
            }
            if let Some((_, first)) = seen.iter().find(|(c, _)| *c == cell) {
                found.push(format!("{page}:{n}: row {cell} repeats line {first}"));
            } else {
                seen.push((cell, n));
            }
        }
    }
    found
}

/// Returns the workspace version of a `Cargo.toml`, or the reason it has none.
///
/// The reason mirrors the Python message: a `KeyError` shows the quoted key,
/// and a parse error shows its message and position.
fn workspace_version(cargo_toml: &str) -> std::result::Result<String, String> {
    let table: toml::Table = cargo_toml.parse().map_err(|err: toml::de::Error| {
        let message = err.message().trim_end().to_owned();
        match err.span() {
            Some(span) => {
                let line = line_of(cargo_toml, span.start.min(cargo_toml.len()));
                let line_start = cargo_toml[..span.start.min(cargo_toml.len())]
                    .rfind('\n')
                    .map_or(0, |i| i + 1);
                let column = cargo_toml[line_start..span.start.min(cargo_toml.len())]
                    .chars()
                    .count()
                    + 1;
                format!("{message} (at line {line}, column {column})")
            }
            None => message,
        }
    })?;
    let mut value: Option<&toml::Value> = None;
    for key in ["workspace", "package", "version"] {
        let next = value.map_or_else(|| table.get(key), |v| v.as_table().and_then(|t| t.get(key)));
        value = Some(next.ok_or_else(|| format!("'{key}'"))?);
    }
    Ok(match value {
        Some(toml::Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
        None => String::new(),
    })
}

/// Checks that the newest version heading of the release notes is the workspace version.
fn release_notes_findings(pats: &Patterns, notes: &str, cargo_toml: &str) -> Vec<String> {
    let outside: Vec<String> = markdown_lines(pats, notes)
        .into_iter()
        .map(|(_, l)| l)
        .collect();
    let outside = outside.join("\n");
    let Some(newest) = pats
        .version_heading
        .captures(&outside)
        .and_then(|c| c.get(1))
    else {
        return Vec::new();
    };
    let newest = newest.as_str();
    match workspace_version(cargo_toml) {
        Err(err) => vec![format!(
            "Cargo.toml: no [workspace.package] version ({err})"
        )],
        Ok(version) if newest != version => vec![format!(
            "RELEASE_NOTES.md: newest heading is {newest}, the workspace is {version}. CHANGELOG.md holds the release history."
        )],
        Ok(_) => Vec::new(),
    }
}

/// Returns a message for each stale-claim pin that still matches.
///
/// `read(path)` returns the text, or `None` for a missing file.
fn stale_claim_findings(
    read: impl Fn(&str) -> Result<Option<String>>,
    claims: &[(&str, &str, &str)],
) -> Result<Vec<String>> {
    let mut found = Vec::new();
    for (path, pattern, fact) in claims {
        let Some(text) = read(path)? else {
            found.push(format!(
                "{path}: missing. Move or drop its STALE_CLAIMS entry."
            ));
            continue;
        };
        for m in pycompat::regex(pattern)?.find_iter(&text) {
            found.push(format!(
                "{path}:{}: stale claim. Fact: {fact}.",
                line_of(&text, m.start())
            ));
        }
    }
    Ok(found)
}

/// Reads a repository file, or returns `None` when it is not a file.
fn read_repo(root: &Path, rel: &str) -> Result<Option<String>> {
    let path = root.join(rel);
    if path.is_file() {
        pycompat::read_text(&path).map(Some)
    } else {
        Ok(None)
    }
}

/// Returns each finding of the repository at `root`.
fn findings(root: &Path) -> Result<Vec<String>> {
    let pats = Patterns::new()?;
    let mut found = Vec::new();
    let architecture = read_repo(root, "docs/architecture.md")?;
    let cargo = read_repo(root, "Cargo.toml")?;
    for (rel, text) in [
        ("docs/architecture.md", &architecture),
        ("Cargo.toml", &cargo),
    ] {
        if text.is_none() {
            found.push(format!("{rel}: missing. Update {THIS_MODULE}."));
        }
    }
    if let Some(text) = &architecture {
        found.extend(event_row_count_findings(
            &pats,
            "docs/architecture.md",
            text,
        ));
        found.extend(duplicate_row_findings(&pats, "docs/architecture.md", text));
    }
    if let Some(cargo) = &cargo {
        let notes = read_repo(root, "RELEASE_NOTES.md")?.unwrap_or_default();
        found.extend(release_notes_findings(&pats, &notes, cargo));
    }
    found.extend(stale_claim_findings(
        |rel| read_repo(root, rel),
        STALE_CLAIMS,
    )?);
    Ok(found)
}

/// Runs `cargo xtask audit doc-claim-drift`.
pub fn run(args: Args) -> Result<ExitCode> {
    if args.self_test {
        return Ok(match self_test() {
            Ok(()) => {
                println!("doc-claim-drift self-test: OK");
                ExitCode::SUCCESS
            }
            Err(message) => {
                eprintln!("doc-claim-drift self-test: FAILED: {message}");
                ExitCode::FAILURE
            }
        });
    }
    let found = findings(&util::repo_root())?;
    for message in &found {
        println!("{message}");
    }
    println!("doc-claim-drift: {} findings", found.len());
    Ok(util::gate_exit(found.is_empty()))
}

/// Fails the self-test with a message when a condition does not hold.
macro_rules! check {
    ($cond:expr, $($fmt:tt)+) => {
        if !$cond {
            return Err(format!($($fmt)+));
        }
    };
}

/// Returns the line numbers of findings, the second `:`-separated field.
fn lines_of(found: &[String]) -> Vec<&str> {
    found
        .iter()
        .map(|m| m.split(':').nth(1).unwrap_or(""))
        .collect()
}

/// Runs the fixtures of the Python `--self-test` mode.
#[allow(clippy::too_many_lines)]
fn self_test() -> std::result::Result<(), String> {
    let pats = Patterns::new().map_err(|e| format!("{e:#}"))?;
    let p = &pats;
    let rows = concat!(
        "| Module | Phase | Purpose |\n|---|---|---|\n",
        "| `event.rs` | 1 | `WorkflowEvent` enum (41 variants, tagged). |\n",
        "| `event.rs` | 1 | `WorkflowEvent` enum — 1,050 variants. |\n",
        "| `event.rs` | 1 | `WorkflowEvent` enum. Variants: 49. |\n",
        "| `event.rs` | 1 | `WorkflowEvent` enum (49 Variants). |\n",
        "| `event.rs` | 1 | `WorkflowEvent` enum. Variants added in issue #140. |\n",
        "```\n| `event.rs` | 1 | 7 variants, in a sample. |\n```\n",
        "| `queue.rs` | 2 | Handles 3 variants of `TaskType`. |\n",
        "Prose: `WorkflowEvent` has 2 variants for sessions.\n",
    );
    let found = event_row_count_findings(p, "x.md", rows);
    check!(lines_of(&found) == ["3", "4", "5", "6"], "{found:?}");
    check!(
        found[0].contains("41 variants") && found[1].contains("1,050 variants"),
        "{found:?}"
    );

    // Legal GFM forms: up to 3 spaces of indent, no outer pipes.
    let indented = "   | Module | Purpose |\n   |---|---|\n   | `event.rs` | 50 variants |\n";
    check!(
        event_row_count_findings(p, "x.md", indented).len() == 1,
        "indented"
    );
    let bare = "Module | Purpose\n:--- | ---\n`event.rs` | 50 variants\n`a.rs` | x\n`a.rs` | y\n";
    check!(event_row_count_findings(p, "x.md", bare).len() == 1, "bare");
    let found = duplicate_row_findings(p, "x.md", bare);
    check!(found.len() == 1 && found[0].contains("x.md:5"), "{found:?}");
    // Four spaces of indent make a code block, not a table.
    let code = "    | M | P |\n    |---|---|\n    | `event.rs` | 50 variants |\n";
    check!(event_row_count_findings(p, "x.md", code).is_empty(), "code");

    let table = concat!(
        "| Module | P |\n|---|---|\n| `a.rs` | 1 |\n| `a.rs` | 2 |\n",
        "| `x \\| y` | 1 |\n| `x \\| z` | 1 |\n\n| `a.rs` | 3 |\n",
    );
    let found = duplicate_row_findings(p, "x.md", table);
    check!(found.len() == 1 && found[0].contains("x.md:4"), "{found:?}");
    let styled = "| M | P |\n|---|---|\n| `event.rs` | 1 |\n| **event.rs** | 2 |\n";
    let found = duplicate_row_findings(p, "x.md", styled);
    check!(found.len() == 1 && found[0].contains("x.md:4"), "{found:?}");
    let sample = "```text\n| state |\n| state |\n```\n| `b.rs` | 1 |\n";
    check!(
        duplicate_row_findings(p, "x.md", sample).is_empty(),
        "sample"
    );
    let split = "| `c.rs` | 1 |\n```\nx\n```\n| `c.rs` | 2 |\n";
    check!(duplicate_row_findings(p, "x.md", split).is_empty(), "split");
    let nested = "````md\n```\n| d |\n```\n| d |\n````\n| `e.rs` | 1 |\n";
    check!(
        duplicate_row_findings(p, "x.md", nested).is_empty(),
        "nested"
    );
    let numbers = |text: &str| -> Vec<usize> {
        markdown_lines(p, text)
            .into_iter()
            .map(|(n, _)| n)
            .collect()
    };
    check!(numbers(nested) == [7], "nested lines");
    check!(numbers("~~~\n```\nx\n~~~\ny\n") == [5], "tilde lines");
    check!(
        numbers("```\nx\n``` not a close\n```\ny\n") == [5],
        "close lines"
    );
    let hidden = concat!(
        "<!--\n| M | P |\n|---|---|\n| `event.rs` | 41 variants |\n| `event.rs` | x |\n-->\n",
        "a <!-- b --> c\n",
    );
    check!(
        event_row_count_findings(p, "x.md", hidden).is_empty(),
        "hidden"
    );
    check!(
        duplicate_row_findings(p, "x.md", hidden).is_empty(),
        "hidden rows"
    );
    let last = markdown_lines(p, hidden).pop();
    check!(last == Some((7, "a  c".to_owned())), "{last:?}");
    let spans = "Use `<!--` to start one.\n| M | P |\n|---|---|\n| `event.rs` | 9 variants |\n";
    check!(
        event_row_count_findings(p, "x.md", spans).len() == 1,
        "spans"
    );
    let wrapped =
        "Use `<!--\n` to start one.\n\n| M | P |\n|---|---|\n| `event.rs` | 9 variants |\n";
    check!(
        event_row_count_findings(p, "x.md", wrapped).len() == 1,
        "wrapped"
    );
    // An unclosed tick is reset at the blank line, so a later comment still hides.
    let lone = "A lone ` tick.\n\n<!--\n| M | P |\n|---|---|\n| `event.rs` | 9 variants |\n-->\n";
    check!(event_row_count_findings(p, "x.md", lone).is_empty(), "lone");
    // An unmatched tick in the same paragraph as a comment stays literal.
    let literal = "A lone ` tick.\n<!--\n| M | P |\n|---|---|\n| `event.rs` | 9 variants |\n-->\n";
    check!(
        event_row_count_findings(p, "x.md", literal).is_empty(),
        "literal"
    );
    let labels = concat!(
        "| M | P |\n|---|---|\n| event.rs | 49 variants |\n| **event.rs** | 49 variants |\n",
        "| [`event.rs`](../src/event.rs) | 49 variants |\n| `src/event.rs` | 49 variants |\n",
        "| `replay_event.rs` | 49 variants |\n",
    );
    let found = event_row_count_findings(p, "x.md", labels);
    check!(lines_of(&found) == ["3", "4", "5", "6"], "{found:?}");
    let formatted = concat!(
        "| M | P |\n|---|---|\n| `event.rs` | Variants: **49** |\n",
        "| `event.rs` | `50` variants |\n| `event.rs` | `last_error` field |\n",
    );
    let found = event_row_count_findings(p, "x.md", formatted);
    check!(lines_of(&found) == ["3", "4"], "{found:?}");

    let cargo = "[workspace]\nmembers = []\n\n[workspace.package]\nauthors = [\"A\"]\nversion = \"0.7.0\"\n";
    let notes = |text: &str| release_notes_findings(p, text, cargo);
    check!(notes("").is_empty(), "empty notes");
    check!(notes("Pointer to CHANGELOG.md.\n").is_empty(), "pointer");
    check!(
        notes("## [0.7.0] - x\n## [0.6.0] - y\n").is_empty(),
        "newest first"
    );
    check!(
        notes("```\n## [0.1.0]\n```\n## 0.7.0\n").is_empty(),
        "fenced heading"
    );
    check!(
        notes("````\n```\n## [0.1.0]\n````\n## 0.7.0\n").is_empty(),
        "nested fence"
    );
    check!(notes("## 0.7.0: Release notes\n").is_empty(), "punctuation");
    check!(notes("   ## [0.6.0]\n").len() == 1, "indented heading");
    check!(notes("    ## [0.6.0]\n").is_empty(), "code heading");
    check!(notes("## v0.7.0.\n").is_empty(), "v prefix");
    let found = notes("## [0.7.0-rc.1+b.5] - x\n");
    check!(
        found.len() == 1 && found[0].contains("0.7.0-rc.1+b.5"),
        "{found:?}"
    );
    check!(
        notes("<!--\n## [0.4.0] - x\n-->\n## [0.7.0]\n").is_empty(),
        "comment heading"
    );
    let found = notes("<!-- -->\n## [0.4.0] - x\n");
    check!(found.len() == 1 && found[0].contains("0.4.0"), "{found:?}");

    let old_claims: &[(&str, &str)] = &[
        (
            "docs/comparison.md",
            concat!(
                "**Planned:** cross-region DR via logical replication.\n",
                "  Planned R&D: [#954](u)\n",
                "There is no built-in multi-region replication or failover today.\n",
                "- **Single-region — no multi-region DR or replication.** Text.\n",
                "cross-shard workflows are explicitly out of scope per the contract.\n",
                "and cross-shard workflows are out of\n  scope by design.\n",
                "- **No cross-shard workflows.** A single workflow's state.\n",
                "No cell on this page\n  claims a throughput or latency comparison.\n",
            ),
        ),
        (
            "docs/architecture.md",
            concat!(
                "Cross-shard rebalancing of existing workflows is out of scope.\n",
                "(cross-shard child fan-out is out of scope, consistent with it).\n",
            ),
        ),
        (
            "docs/sharding.md",
            concat!(
                "out of scope (cross-shard rebalancing is not supported).\n",
                "per-shard worker assignment, geo-replication / cross-region failover, and\n",
            ),
        ),
    ];
    let lookup = |files: &'static [(&'static str, &'static str)]| {
        move |path: &str| -> Result<Option<String>> {
            Ok(files
                .iter()
                .find(|(p, _)| *p == path)
                .map(|(_, t)| (*t).to_owned()))
        }
    };
    let claims: Vec<(&str, &str, &str)> = STALE_CLAIMS
        .iter()
        .copied()
        .filter(|c| old_claims.iter().any(|(p, _)| *p == c.0))
        .collect();
    let found = stale_claim_findings(lookup(old_claims), &claims).map_err(|e| format!("{e:#}"))?;
    check!(found.len() == 12, "{found:?}");

    // True statements that a pin must not match.
    let current: &'static [(&'static str, &'static str)] = &[
        (
            "docs/comparison.md",
            concat!(
                "| DBOS | DBOS has no cross-region replication or failover. |\n",
                "Unlike Harvest, DBOS has no cross-region replication or failover.\n",
                "Unplanned failover is covered by [#954](u).\n",
                "There is no automatic regional failover.\n",
                "Cross-shard composition is limited.\n",
            ),
        ),
        (
            "docs/architecture.md",
            concat!(
                "Cross-shard rebalancing is not supported for running workflows.\n",
                "It ended at *\"cross-shard rebalancing of existing\nworkflows is out of scope\"*.\n",
            ),
        ),
        (
            "docs/sharding.md",
            "A running workflow cannot move. Cross-region failover is in cross-region-dr.md.\n",
        ),
    ];
    let found = stale_claim_findings(lookup(current), &claims).map_err(|e| format!("{e:#}"))?;
    check!(found.is_empty(), "{found:?}");

    let found = stale_claim_findings(lookup(&[]), &[("gone.md", "x", "y")])
        .map_err(|e| format!("{e:#}"))?;
    check!(
        found.len() == 1 && found[0].contains("missing"),
        "{found:?}"
    );

    // Every real pattern compiles.
    for (_, pattern, _) in STALE_CLAIMS {
        pycompat::regex(pattern).map_err(|e| format!("{e:#}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_self_test_fixtures_pass() {
        assert_eq!(self_test(), Ok(()));
    }

    #[test]
    fn a_missing_architecture_page_names_this_module() {
        let dir = smop::fs::temp_dir().unwrap();
        let found = findings(dir.path()).unwrap();
        assert_eq!(
            found[0],
            format!("docs/architecture.md: missing. Update {THIS_MODULE}.")
        );
        assert_eq!(
            found[1],
            format!("Cargo.toml: missing. Update {THIS_MODULE}.")
        );
        assert!(
            found[2..]
                .iter()
                .all(|m| m.ends_with("missing. Move or drop its STALE_CLAIMS entry."))
        );
    }

    #[test]
    fn a_stale_version_heading_is_reported() {
        let dir = smop::fs::temp_dir().unwrap();
        std::fs::write(
            dir.path().join("Cargo.toml"),
            "[workspace.package]\nversion = \"0.8.0\"\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("RELEASE_NOTES.md"), "## [0.7.0]\n").unwrap();
        let found = findings(dir.path()).unwrap();
        assert!(found.contains(
            &"RELEASE_NOTES.md: newest heading is 0.7.0, the workspace is 0.8.0. CHANGELOG.md holds the release history."
                .to_owned()
        ));
    }

    #[test]
    fn a_missing_version_key_reads_like_a_key_error() {
        assert_eq!(
            workspace_version("[workspace]\n"),
            Err("'package'".to_owned())
        );
        assert!(workspace_version("x =").unwrap_err().contains("(at line 1"));
    }
}
