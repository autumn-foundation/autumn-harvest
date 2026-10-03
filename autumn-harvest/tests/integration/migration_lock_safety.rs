//! Online-migration lock-safety lint (issue #1810). No DB, no feature gate.
//!
//! A migration that locks a hot table stops the engine for as long as it
//! holds or waits for that lock. This lint reads each `up.sql` after
//! `LOCK_SAFETY_CUTOFF` and enforces two rules on the hot tables:
//!
//!   1. `lock-timeout`: a statement that takes a blocking lock on a hot table
//!      must come after a non-zero `lock_timeout`. Without one, the statement
//!      waits behind a long transaction, and every write queues behind it.
//!   2. `blocking-index`: `CREATE INDEX`, `DROP INDEX` and `REINDEX` on a hot
//!      table must use `CONCURRENTLY`. The plain forms block writes, or all
//!      access, for the whole build.
//!
//! An in-file annotation is the reviewed escape hatch. Shipped migrations
//! cannot change, so they are grandfathered by name in `GRANDFATHERED`.
//! `docs/upgrading/online-migrations.md` is the author guide.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// The last migration this lint does not bind, inclusive.
///
/// Issue #1810 names `20260915231809` as the first offender. That migration
/// must stay in scope, so the cutoff is the migration just before it.
const LOCK_SAFETY_CUTOFF: &str = "20260914165542";

/// The newest migration that `GRANDFATHERED` may name.
///
/// This is the newest migration on disk when the lint landed. A newer
/// migration cannot be grandfathered, so it uses the in-file annotation.
const GRANDFATHER_CEILING: &str = "20261002033903";

/// Tables that every workflow step reads or writes.
///
/// A blocking lock on one of them stalls the engine, not one feature.
/// `harvest_workflow_outbox` lives in the application database, and the
/// application writes it in its own transactions.
const HOT_TABLES: &[&str] = &[
    "harvest_audit_log",
    "harvest_events",
    "harvest_signals",
    "harvest_task_queue",
    "harvest_timers",
    "harvest_workflow_executions",
    "harvest_workflow_outbox",
];

/// Migration trees, relative to the workspace root.
///
/// The plugin `harvest` tree runs against the same database as the core tree.
/// The `app` tree runs against the application database.
const MIGRATION_TREES: &[&str] = &[
    "autumn-harvest/migrations",
    "autumn-harvest-plugin/migrations/harvest",
    "autumn-harvest-plugin/migrations/app",
];

/// The comment prefix of an allow annotation.
const ANNOTATION_PREFIX: &str = "lock-safety:";

/// Shipped migrations after the cutoff that break a rule, with the reason.
///
/// Each entry is a migration directory name and the rule it breaks. A test
/// fails when an entry no longer matches a finding, so the list cannot rot.
const GRANDFATHERED: &[(&str, Rule, &str)] = &[
    (
        "20260915231809_harvest_migrated_seal_terminal_at",
        Rule::BlockingIndex,
        "Drops and rebuilds the active-uniqueness index in the migration \
         transaction. Shipped before this lint.",
    ),
    (
        "20260915231809_harvest_migrated_seal_terminal_at",
        Rule::LockTimeout,
        "Adds two columns with no lock bound. Shipped before this lint.",
    ),
    (
        "20260916151612_harvest_staging_vacated_state",
        Rule::LockTimeout,
        "Adds a nullable column with no lock bound. Shipped before this lint.",
    ),
    (
        "20260919144514_harvest_events_recent_by_timestamp_index",
        Rule::BlockingIndex,
        "Guarded plain build. The upgrade guide tells operators to prebuild \
         the index with CONCURRENTLY, and the guard accepts it.",
    ),
    (
        "20260919144514_harvest_events_recent_by_timestamp_index",
        Rule::LockTimeout,
        "The guarded plain build has no lock bound. Shipped before this lint.",
    ),
    (
        "20260920014641_harvest_staging_vacated_by",
        Rule::LockTimeout,
        "Adds a nullable column with no lock bound. Shipped before this lint.",
    ),
    (
        "20260921011505_harvest_task_queue_timer_fires_at",
        Rule::LockTimeout,
        "Adds a column and backfills it with no lock bound. Shipped before \
         this lint.",
    ),
    (
        "20261001192155_harvest_quota_reconcile_name_id_index",
        Rule::BlockingIndex,
        "Guarded plain build. The upgrade guide tells operators to prebuild \
         the index with CONCURRENTLY, and the guard accepts it.",
    ),
    (
        "20261001192155_harvest_quota_reconcile_name_id_index",
        Rule::LockTimeout,
        "The guarded plain build has no lock bound. Shipped before this lint.",
    ),
];

/// One lint rule. The id is what an annotation and a failure message use.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Rule {
    /// A blocking lock on a hot table with no `lock_timeout` before it.
    LockTimeout,
    /// A plain `CREATE INDEX`, `DROP INDEX` or `REINDEX` on a hot table.
    BlockingIndex,
    /// `CONCURRENTLY` in a migration that runs inside a transaction.
    ConcurrentlyInTransaction,
    /// A `lock-safety:` comment that does not parse.
    BadAnnotation,
    /// A valid annotation that suppresses no finding.
    UnusedAnnotation,
}

impl Rule {
    const ALL: [Self; 5] = [
        Self::LockTimeout,
        Self::BlockingIndex,
        Self::ConcurrentlyInTransaction,
        Self::BadAnnotation,
        Self::UnusedAnnotation,
    ];

    const fn id(self) -> &'static str {
        match self {
            Self::LockTimeout => "lock-timeout",
            Self::BlockingIndex => "blocking-index",
            Self::ConcurrentlyInTransaction => "concurrently-in-transaction",
            Self::BadAnnotation => "bad-annotation",
            Self::UnusedAnnotation => "unused-annotation",
        }
    }

    fn from_id(id: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|rule| rule.id() == id)
    }

    /// Whether an annotation or a grandfather entry can suppress this rule.
    ///
    /// The annotation rules guard the escape hatch itself, so nothing
    /// suppresses them.
    const fn allowable(self) -> bool {
        matches!(
            self,
            Self::LockTimeout | Self::BlockingIndex | Self::ConcurrentlyInTransaction
        )
    }
}

/// One rule violation in one migration.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Finding {
    rule: Rule,
    /// The 1-based line of the statement or annotation.
    line: usize,
    detail: String,
}

/// Lint one `up.sql`.
///
/// `index_tables` maps each index name to its table, from `index_tables()`.
/// A `DROP INDEX` needs it, because the statement does not name the table.
fn lint(
    sql: &str,
    run_in_transaction: bool,
    index_tables: &BTreeMap<String, String>,
) -> Vec<Finding> {
    let (toks, comments) = tokenize(sql);
    let created = created_tables(&toks);
    let is_hot = |table: Option<&str>| match table {
        // An index that no migration creates has an unknown table. Fail closed.
        None => true,
        Some(t) => HOT_TABLES.contains(&t) && !created.contains(t),
    };

    let mut findings = Vec::new();
    // `statements` returns hits in source order, so the first hot hit is the
    // first lock.
    let mut first_lock: Option<(&Hit, &str)> = None;
    let hits = statements(&toks);
    for hit in &hits {
        let table = hit.table.as_deref().or_else(|| {
            hit.index
                .as_ref()
                .and_then(|i| index_tables.get(i))
                .map(String::as_str)
        });
        let hot = is_hot(table);
        let table = table.unwrap_or("an index that no migration creates");
        match hit.kind {
            Kind::Index { concurrent: true } => {
                if run_in_transaction {
                    findings.push(Finding {
                        rule: Rule::ConcurrentlyInTransaction,
                        line: hit.line,
                        detail: format!(
                            "{} CONCURRENTLY cannot run in a transaction. Set \
                             `run_in_transaction = false` in metadata.toml.",
                            hit.verb
                        ),
                    });
                }
                continue;
            }
            Kind::Index { concurrent: false } if hot => findings.push(Finding {
                rule: Rule::BlockingIndex,
                line: hit.line,
                detail: format!(
                    "plain {} on {table} blocks the table for the whole build",
                    hit_label(hit)
                ),
            }),
            _ => {}
        }
        if hot && first_lock.is_none() {
            first_lock = Some((hit, table));
        }
    }

    if let Some((lock, table)) = first_lock {
        if first_lock_timeout(&toks, run_in_transaction).is_none_or(|at| at > lock.at) {
            findings.push(Finding {
                rule: Rule::LockTimeout,
                line: lock.line,
                detail: format!(
                    "{} locks {table} with no non-zero lock_timeout set before it",
                    hit_label(lock)
                ),
            });
        }
    }

    apply_annotations(sql, &comments, findings)
}

/// A short label for a statement in a failure message.
fn hit_label(hit: &Hit) -> String {
    match &hit.index {
        Some(index) => format!("{} {index}", hit.verb),
        None => hit.verb.to_string(),
    }
}

/// Map each index that a migration creates to its table.
fn index_tables<'a>(sqls: impl IntoIterator<Item = &'a str>) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for sql in sqls {
        let (toks, _) = tokenize(sql);
        for hit in statements(&toks) {
            if let (Some(index), Some(table)) = (hit.index, hit.table) {
                map.insert(index, table);
            }
        }
    }
    map
}

// ── Annotations ──────────────────────────────────────────────────────────────

/// A valid allow annotation.
struct Annotation {
    rule: Rule,
    line: usize,
    used: bool,
}

/// Parse one `--` comment as an annotation.
///
/// Returns `None` for an ordinary comment. The form is
/// `lock-safety: allow <rule> #<issue> <reason>`.
fn parse_annotation(comment: &str) -> Option<Result<Rule, String>> {
    let body = comment.trim().strip_prefix(ANNOTATION_PREFIX)?;
    let mut words = body.split_whitespace();
    let parsed = (|| {
        if words.next() != Some("allow") {
            return Err("an annotation must start with `allow`".to_string());
        }
        let id = words.next().unwrap_or("");
        let rule = Rule::from_id(id)
            .filter(|rule| rule.allowable())
            .ok_or_else(|| format!("`{id}` is not a rule an annotation can allow"))?;
        let issue = words.next().unwrap_or("");
        let digits = issue.strip_prefix('#').unwrap_or("");
        if digits.is_empty() || !digits.chars().all(|c| c.is_ascii_digit()) {
            return Err("an annotation must cite an issue as `#<number>`".to_string());
        }
        if words.next().is_none() {
            return Err("an annotation must give a reason after the issue".to_string());
        }
        Ok(rule)
    })();
    Some(parsed)
}

/// Drop each finding that an annotation directly above it allows.
///
/// "Directly above" means only `--` comment lines sit between the annotation
/// and the statement. A blank line or code line ends the search.
fn apply_annotations(sql: &str, comments: &[Comment], findings: Vec<Finding>) -> Vec<Finding> {
    let comment_lines: BTreeSet<usize> = sql
        .lines()
        .enumerate()
        .filter(|(_, line)| line.trim_start().starts_with("--"))
        .map(|(i, _)| i + 1)
        .collect();

    let mut out = Vec::new();
    let mut annotations = Vec::new();
    for comment in comments {
        match parse_annotation(&comment.text) {
            None => {}
            Some(Ok(rule)) => annotations.push(Annotation {
                rule,
                line: comment.line,
                used: false,
            }),
            Some(Err(detail)) => out.push(Finding {
                rule: Rule::BadAnnotation,
                line: comment.line,
                detail,
            }),
        }
    }

    for finding in findings {
        let mut allowed = false;
        let mut line = finding.line;
        while line > 1 && comment_lines.contains(&(line - 1)) {
            line -= 1;
            for annotation in annotations
                .iter_mut()
                .filter(|a| a.line == line && a.rule == finding.rule)
            {
                annotation.used = true;
                allowed = true;
            }
        }
        if !allowed {
            out.push(finding);
        }
    }

    out.extend(annotations.iter().filter(|a| !a.used).map(|a| Finding {
        rule: Rule::UnusedAnnotation,
        line: a.line,
        detail: format!(
            "allows {} but the statement below needs no such allowance",
            a.rule.id()
        ),
    }));
    out.sort_by_key(|f| (f.line, f.rule));
    out
}

// ── Lexer ────────────────────────────────────────────────────────────────────

/// One lexical token of an `up.sql`.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Tok {
    /// A keyword or an identifier. An unquoted word is lowercased.
    Word(String),
    /// The value of a string literal.
    Str(String),
    /// Any other character.
    Punct(char),
}

struct Token {
    tok: Tok,
    line: usize,
}

/// A `--` comment, without the dashes.
struct Comment {
    text: String,
    line: usize,
}

/// Split `sql` into tokens and `--` comments.
///
/// Comments and string literals never become words, so prose cannot match a
/// statement. A dollar-quoted body is scanned as code, because a `DO $$`
/// block holds real DDL. That also scans a dollar-quoted string literal, which
/// fails closed.
fn tokenize(sql: &str) -> (Vec<Token>, Vec<Comment>) {
    let chars: Vec<char> = sql.chars().collect();
    let at = |i: usize| chars.get(i).copied();
    let mut toks = Vec::new();
    let mut comments = Vec::new();
    let mut line = 1;
    let mut i = 0;
    while let Some(c) = at(i) {
        let next = at(i + 1);
        if c == '\n' {
            line += 1;
            i += 1;
        } else if c.is_whitespace() {
            i += 1;
        } else if c == '-' && next == Some('-') {
            let start = i + 2;
            while at(i).is_some_and(|c| c != '\n') {
                i += 1;
            }
            comments.push(Comment {
                text: chars[start..i].iter().collect(),
                line,
            });
        } else if c == '/' && next == Some('*') {
            // Block comments nest in Postgres.
            let mut depth = 0;
            while let Some(c) = at(i) {
                if c == '/' && at(i + 1) == Some('*') {
                    depth += 1;
                    i += 2;
                } else if c == '*' && at(i + 1) == Some('/') {
                    depth -= 1;
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    line += usize::from(c == '\n');
                    i += 1;
                }
            }
        } else if c == '\'' || (matches!(c, 'e' | 'E') && next == Some('\'')) {
            // An `E'...'` string also takes backslash escapes.
            let escapes = c != '\'';
            let start_line = line;
            i += if escapes { 2 } else { 1 };
            let mut value = String::new();
            while let Some(c) = at(i) {
                line += usize::from(c == '\n');
                if escapes && c == '\\' {
                    if let Some(escaped) = at(i + 1) {
                        line += usize::from(escaped == '\n');
                        value.push(escaped);
                    }
                    i += 2;
                } else if c == '\'' && at(i + 1) == Some('\'') {
                    value.push('\'');
                    i += 2;
                } else if c == '\'' {
                    i += 1;
                    break;
                } else {
                    value.push(c);
                    i += 1;
                }
            }
            toks.push(Token {
                tok: Tok::Str(value),
                line: start_line,
            });
        } else if c == '"' {
            // A quoted identifier keeps its case. `""` is an escaped quote.
            i += 1;
            let mut value = String::new();
            while let Some(c) = at(i) {
                if c == '"' && at(i + 1) == Some('"') {
                    value.push('"');
                    i += 2;
                } else if c == '"' {
                    i += 1;
                    break;
                } else {
                    line += usize::from(c == '\n');
                    value.push(c);
                    i += 1;
                }
            }
            toks.push(Token {
                tok: Tok::Word(value),
                line,
            });
        } else if let Some(len) = dollar_tag_len(&chars[i..]) {
            i += len;
        } else if c.is_alphanumeric() || c == '_' {
            let start = i;
            while at(i).is_some_and(|c| c.is_alphanumeric() || c == '_' || c == '$') {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            toks.push(Token {
                tok: Tok::Word(word.to_lowercase()),
                line,
            });
        } else {
            toks.push(Token {
                tok: Tok::Punct(c),
                line,
            });
            i += 1;
        }
    }
    (toks, comments)
}

/// The length of a dollar-quote delimiter such as `$$` or `$body$`.
///
/// Returns `None` for anything else, such as a `$1` parameter.
fn dollar_tag_len(rest: &[char]) -> Option<usize> {
    if rest.first() != Some(&'$') {
        return None;
    }
    let tag = rest[1..]
        .iter()
        .take_while(|c| c.is_ascii_alphanumeric() || **c == '_')
        .count();
    if tag > 0 && rest[1].is_ascii_digit() {
        return None;
    }
    (rest.get(1 + tag) == Some(&'$')).then_some(tag + 2)
}

// ── Statement matching ───────────────────────────────────────────────────────

/// What a matched statement does to its table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    /// `CREATE INDEX`, `DROP INDEX` or `REINDEX`.
    Index { concurrent: bool },
    /// Any other statement that takes a blocking lock.
    Lock,
}

/// One statement that can lock a table.
#[derive(Debug)]
struct Hit {
    /// The token index of the statement's first word.
    at: usize,
    line: usize,
    verb: &'static str,
    /// The index the statement names, if any.
    index: Option<String>,
    /// The table the statement names. `None` when only `index` is known.
    table: Option<String>,
    kind: Kind,
}

fn word(toks: &[Token], k: usize) -> Option<&str> {
    match &toks.get(k)?.tok {
        Tok::Word(w) => Some(w),
        _ => None,
    }
}

fn is_word(toks: &[Token], k: usize, expected: &str) -> bool {
    word(toks, k) == Some(expected)
}

fn is_punct(toks: &[Token], k: usize, expected: char) -> bool {
    toks.get(k).is_some_and(|t| t.tok == Tok::Punct(expected))
}

fn string(toks: &[Token], k: usize) -> Option<&str> {
    match &toks.get(k)?.tok {
        Tok::Str(s) => Some(s),
        _ => None,
    }
}

/// Read a name that may carry a schema. Return its last part and the next index.
fn qualified_name(toks: &[Token], k: usize) -> Option<(String, usize)> {
    let mut name = word(toks, k)?;
    let mut k = k + 1;
    while is_punct(toks, k, '.') {
        let Some(part) = word(toks, k + 1) else {
            break;
        };
        name = part;
        k += 2;
    }
    Some((name.to_string(), k))
}

/// Read a comma-separated list of names. Each name may carry `ONLY`.
fn name_list(toks: &[Token], mut k: usize) -> Vec<String> {
    let mut names = Vec::new();
    loop {
        if is_word(toks, k, "only") {
            k += 1;
        }
        let Some((name, next)) = qualified_name(toks, k) else {
            break;
        };
        names.push(name);
        if !is_punct(toks, next, ',') {
            break;
        }
        k = next + 1;
    }
    names
}

/// Skip `IF EXISTS` or `IF NOT EXISTS` at `k`.
fn skip_if_exists(toks: &[Token], k: usize) -> usize {
    if !is_word(toks, k, "if") {
        return k;
    }
    let j = if is_word(toks, k + 1, "not") {
        k + 2
    } else {
        k + 1
    };
    if is_word(toks, j, "exists") { j + 1 } else { k }
}

/// Tables that `toks` creates. A lock on a brand-new table blocks no one.
fn created_tables(toks: &[Token]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for k in 0..toks.len() {
        if !is_word(toks, k, "create") {
            continue;
        }
        let mut j = k + 1;
        if ["temp", "temporary", "unlogged"]
            .iter()
            .any(|w| is_word(toks, j, w))
        {
            j += 1;
        }
        if is_word(toks, j, "table") {
            if let Some((name, _)) = qualified_name(toks, skip_if_exists(toks, j + 1)) {
                out.insert(name);
            }
        }
    }
    out
}

/// Every statement in `toks` that can take a blocking table lock.
fn statements(toks: &[Token]) -> Vec<Hit> {
    let mut hits = Vec::new();
    for k in 0..toks.len() {
        let line = toks[k].line;
        let lock = |verb, table: String| Hit {
            at: k,
            line,
            verb,
            index: None,
            table: Some(table),
            kind: Kind::Lock,
        };
        match word(toks, k) {
            Some("create") => {
                if let Some(hit) = create_index(toks, k) {
                    hits.push(hit);
                } else if let Some(table) = trigger_table(toks, k, k + 1) {
                    hits.push(lock("CREATE TRIGGER", table));
                }
            }
            Some("drop") if is_word(toks, k + 1, "index") => {
                let mut j = k + 2;
                let concurrent = is_word(toks, j, "concurrently");
                j = skip_if_exists(toks, j + usize::from(concurrent));
                for index in name_list(toks, j) {
                    hits.push(Hit {
                        at: k,
                        line,
                        verb: "DROP INDEX",
                        index: Some(index),
                        table: None,
                        kind: Kind::Index { concurrent },
                    });
                }
            }
            Some("drop") if is_word(toks, k + 1, "table") => {
                let j = skip_if_exists(toks, k + 2);
                hits.extend(
                    name_list(toks, j)
                        .into_iter()
                        .map(|t| lock("DROP TABLE", t)),
                );
            }
            Some("drop") if is_word(toks, k + 1, "trigger") => {
                if let Some(table) = trigger_table(toks, k, k + 1) {
                    hits.push(lock("DROP TRIGGER", table));
                }
            }
            Some("alter") if is_word(toks, k + 1, "table") => {
                let mut j = skip_if_exists(toks, k + 2);
                if is_word(toks, j, "only") {
                    j += 1;
                }
                if let Some((table, _)) = qualified_name(toks, j) {
                    hits.push(lock("ALTER TABLE", table));
                }
            }
            Some(verb @ ("lock" | "truncate")) => {
                let j = if is_word(toks, k + 1, "table") {
                    k + 2
                } else {
                    k + 1
                };
                let verb = if verb == "lock" {
                    "LOCK TABLE"
                } else {
                    "TRUNCATE"
                };
                hits.extend(name_list(toks, j).into_iter().map(|t| lock(verb, t)));
            }
            Some("references") => {
                if let Some((table, _)) = qualified_name(toks, k + 1) {
                    hits.push(lock("REFERENCES", table));
                }
            }
            Some("reindex") => hits.extend(reindex(toks, k)),
            _ => {}
        }
    }
    hits
}

/// `CREATE [UNIQUE] INDEX [CONCURRENTLY] [IF NOT EXISTS] [name] ON [ONLY] table`.
fn create_index(toks: &[Token], k: usize) -> Option<Hit> {
    let mut j = k + 1;
    if is_word(toks, j, "unique") {
        j += 1;
    }
    if !is_word(toks, j, "index") {
        return None;
    }
    j += 1;
    let concurrent = is_word(toks, j, "concurrently");
    j = skip_if_exists(toks, j + usize::from(concurrent));
    let mut index = None;
    if !is_word(toks, j, "on") {
        let (name, next) = qualified_name(toks, j)?;
        index = Some(name);
        j = next;
    }
    if !is_word(toks, j, "on") {
        return None;
    }
    j += 1;
    if is_word(toks, j, "only") {
        j += 1;
    }
    let (table, _) = qualified_name(toks, j)?;
    Some(Hit {
        at: k,
        line: toks[k].line,
        verb: "CREATE INDEX",
        index,
        table: Some(table),
        kind: Kind::Index { concurrent },
    })
}

/// The table of `CREATE [OR REPLACE] [CONSTRAINT] TRIGGER ... ON table` or
/// `DROP TRIGGER [IF EXISTS] name ON table`. `j` is the index after the verb.
fn trigger_table(toks: &[Token], k: usize, mut j: usize) -> Option<String> {
    if is_word(toks, k, "create") {
        if is_word(toks, j, "or") && is_word(toks, j + 1, "replace") {
            j += 2;
        }
        if is_word(toks, j, "constraint") {
            j += 1;
        }
    }
    if !is_word(toks, j, "trigger") {
        return None;
    }
    while j < toks.len() && !is_punct(toks, j, ';') {
        if is_word(toks, j, "on") {
            return qualified_name(toks, j + 1).map(|(table, _)| table);
        }
        j += 1;
    }
    None
}

/// `REINDEX [(options)] {INDEX | TABLE} [CONCURRENTLY] name`.
fn reindex(toks: &[Token], k: usize) -> Option<Hit> {
    let mut j = k + 1;
    if is_punct(toks, j, '(') {
        while j < toks.len() && !is_punct(toks, j, ')') {
            j += 1;
        }
        j += 1;
    }
    let on_index = is_word(toks, j, "index");
    if !on_index && !is_word(toks, j, "table") {
        return None;
    }
    j += 1;
    let concurrent = is_word(toks, j, "concurrently");
    let (name, _) = qualified_name(toks, j + usize::from(concurrent))?;
    let (index, table) = if on_index {
        (Some(name), None)
    } else {
        (None, Some(name))
    };
    Some(Hit {
        at: k,
        line: toks[k].line,
        verb: "REINDEX",
        index,
        table,
        kind: Kind::Index { concurrent },
    })
}

/// The token index of the first `lock_timeout` setting that bounds a wait.
///
/// Accepts `SET [LOCAL | SESSION] lock_timeout {= | TO} <value>` and
/// `set_config('lock_timeout', <value>, <is_local>)`. A transaction-local
/// setting does nothing outside a transaction, so it counts only inside one.
fn first_lock_timeout(toks: &[Token], run_in_transaction: bool) -> Option<usize> {
    (0..toks.len()).find(|&k| {
        if is_word(toks, k, "set") {
            let mut j = k + 1;
            let local = is_word(toks, j, "local");
            if local || is_word(toks, j, "session") {
                j += 1;
            }
            if !is_word(toks, j, "lock_timeout") {
                return false;
            }
            j += 1;
            if is_punct(toks, j, '=') || is_word(toks, j, "to") {
                j += 1;
            }
            (run_in_transaction || !local) && bounds_wait(toks, j)
        } else if is_word(toks, k, "set_config")
            && is_punct(toks, k + 1, '(')
            && string(toks, k + 2) == Some("lock_timeout")
            && is_punct(toks, k + 3, ',')
        {
            let local = is_word(toks, k + 6, "true");
            (run_in_transaction || !local) && bounds_wait(toks, k + 4)
        } else {
            false
        }
    })
}

/// Whether the value at `k` is a non-zero timeout. `0` and `DEFAULT` disable it.
fn bounds_wait(toks: &[Token], k: usize) -> bool {
    let value = match toks.get(k).map(|t| &t.tok) {
        Some(Tok::Str(s) | Tok::Word(s)) => s.trim(),
        _ => return false,
    };
    let number: String = value
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    number.parse::<f64>().is_ok_and(|n| n > 0.0)
}

/// Read Diesel's `run_in_transaction` out of a `metadata.toml`.
///
/// `autumn_harvest::migrate` has the full parser, but it needs the `db`
/// feature. This lint runs without it, so it accepts only the one key.
fn run_in_transaction(metadata: &str) -> bool {
    for line in metadata.lines() {
        let line = line.split('#').next().unwrap_or("").trim();
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        if key.trim() == "run_in_transaction" {
            return match value.trim() {
                "true" => true,
                "false" => false,
                other => panic!("run_in_transaction must be true or false, found {other}"),
            };
        }
    }
    true
}

/// One migration read from disk.
struct OnDisk {
    tree: &'static str,
    name: String,
    sql: String,
    run_in_transaction: bool,
}

impl OnDisk {
    /// The Diesel version: the digits before the first `_`.
    fn version(&self) -> &str {
        self.name.split('_').next().unwrap_or(&self.name)
    }

    fn in_scope(&self) -> bool {
        self.version() > LOCK_SAFETY_CUTOFF
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the crate directory has a parent")
        .to_path_buf()
}

/// Read every migration in every tree, sorted by version.
///
/// Any IO error panics. A skipped migration would be an unlinted migration.
fn load_migrations() -> Vec<OnDisk> {
    let root = workspace_root();
    let mut out = Vec::new();
    for tree in MIGRATION_TREES {
        let dir = root.join(tree);
        let entries = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("read migration tree {}: {e}", dir.display()));
        for entry in entries {
            let path = entry
                .unwrap_or_else(|e| panic!("read an entry of {}: {e}", dir.display()))
                .path();
            if !path.is_dir() {
                continue;
            }
            out.push(read_migration(tree, &path));
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

fn read_migration(tree: &'static str, dir: &Path) -> OnDisk {
    let name = dir
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_else(|| panic!("migration directory name is not UTF-8: {}", dir.display()))
        .to_string();
    let up = dir.join("up.sql");
    let sql = std::fs::read_to_string(&up).unwrap_or_else(|e| panic!("read {}: {e}", up.display()));
    let metadata = dir.join("metadata.toml");
    let run_in_transaction = if metadata.is_file() {
        let text = std::fs::read_to_string(&metadata)
            .unwrap_or_else(|e| panic!("read {}: {e}", metadata.display()));
        run_in_transaction(&text)
    } else {
        true
    };
    OnDisk {
        tree,
        name,
        sql,
        run_in_transaction,
    }
}

/// The index map for the real trees.
fn real_index_tables(migrations: &[OnDisk]) -> BTreeMap<String, String> {
    index_tables(migrations.iter().map(|m| m.sql.as_str()))
}

fn grandfathered(name: &str, rule: Rule) -> bool {
    GRANDFATHERED
        .iter()
        .any(|(entry, entry_rule, _)| *entry == name && *entry_rule == rule)
}

/// Lint a synthetic migration with an index map built from `history` and it.
fn lint_with_history(history: &[&str], sql: &str, run_in_transaction: bool) -> Vec<Finding> {
    let map = index_tables(history.iter().copied().chain([sql]));
    lint(sql, run_in_transaction, &map)
}

fn rules(findings: &[Finding]) -> Vec<Rule> {
    findings.iter().map(|f| f.rule).collect()
}

// ── Rule 2: blocking-index ───────────────────────────────────────────────────

#[test]
fn plain_create_index_on_a_hot_table_is_flagged() {
    let sql = "SET LOCAL lock_timeout = '5s';\n\
               CREATE INDEX idx_x ON harvest_events (timestamp);\n";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::BlockingIndex], "{findings:?}");
    assert_eq!(findings[0].line, 2, "the finding names the statement line");
}

#[test]
fn every_create_index_spelling_is_recognised() {
    for sql in [
        "CREATE UNIQUE INDEX idx_x ON harvest_task_queue (id);",
        "create index if not exists idx_x on only public.harvest_task_queue (id);",
        "CREATE INDEX idx_x ON \"harvest_task_queue\" (id);",
        "CREATE INDEX ON harvest_task_queue (id);",
        "CREATE\n    INDEX\n    idx_x\n    ON harvest_task_queue (id);",
    ] {
        let sql = format!("SET LOCAL lock_timeout = '5s';\n{sql}");
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(
            rules(&findings),
            [Rule::BlockingIndex],
            "{sql}: {findings:?}"
        );
    }
}

#[test]
fn concurrent_index_builds_and_cold_tables_pass() {
    let concurrent = "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_x ON harvest_events (id);";
    assert_eq!(lint_with_history(&[], concurrent, false), []);

    let cold = "CREATE INDEX idx_x ON harvest_schedules (id);";
    assert_eq!(lint_with_history(&[], cold, true), []);

    // An exact name match only. A table that shares a hot prefix is cold.
    let prefixed = "CREATE INDEX idx_x ON harvest_events_archive (id);";
    assert_eq!(lint_with_history(&[], prefixed, true), []);
}

#[test]
fn a_table_created_in_the_same_migration_is_not_hot_yet() {
    // No session can hold a lock on a table that does not exist yet.
    let sql = "CREATE TABLE IF NOT EXISTS harvest_signals (id BIGINT);\n\
               CREATE INDEX idx_x ON harvest_signals (id);\n\
               ALTER TABLE harvest_signals ADD COLUMN y INT;\n";
    assert_eq!(lint_with_history(&[], sql, true), []);
}

#[test]
fn drop_index_resolves_its_table_from_earlier_migrations() {
    let history = [
        "CREATE INDEX idx_hot ON harvest_workflow_executions (id);\n\
                    CREATE INDEX idx_cold ON harvest_schedules (id);",
    ];

    let hot = "SET LOCAL lock_timeout = '5s';\nDROP INDEX IF EXISTS public.idx_hot;";
    assert_eq!(
        rules(&lint_with_history(&history, hot, true)),
        [Rule::BlockingIndex]
    );

    let cold = "DROP INDEX idx_cold;";
    assert_eq!(lint_with_history(&history, cold, true), []);

    let concurrent = "DROP INDEX CONCURRENTLY IF EXISTS idx_hot;";
    assert_eq!(lint_with_history(&history, concurrent, false), []);
}

#[test]
fn drop_index_of_an_unknown_index_fails_closed() {
    let sql = "SET LOCAL lock_timeout = '5s';\nDROP INDEX idx_nobody_created;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::BlockingIndex], "{findings:?}");
}

#[test]
fn reindex_on_a_hot_table_is_flagged() {
    let history = ["CREATE INDEX idx_hot ON harvest_timers (id);"];
    for sql in ["REINDEX TABLE harvest_timers;", "REINDEX INDEX idx_hot;"] {
        let sql = format!("SET LOCAL lock_timeout = '5s';\n{sql}");
        assert_eq!(
            rules(&lint_with_history(&history, &sql, true)),
            [Rule::BlockingIndex],
            "{sql}"
        );
    }
    let concurrent = "REINDEX INDEX CONCURRENTLY idx_hot;";
    assert_eq!(lint_with_history(&history, concurrent, false), []);
}

// ── Rule 1: lock-timeout ─────────────────────────────────────────────────────

#[test]
fn alter_table_on_a_hot_table_needs_a_lock_timeout() {
    let bare = "ALTER TABLE harvest_task_queue ADD COLUMN x INT;";
    let findings = lint_with_history(&[], bare, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");

    for set in [
        "SET LOCAL lock_timeout = '5s';",
        "SET lock_timeout TO '2s';",
        "SET LOCAL lock_timeout = 5000;",
        "SELECT set_config('lock_timeout', '5s', true);",
    ] {
        let sql = format!("{set}\n{bare}");
        assert_eq!(lint_with_history(&[], &sql, true), [], "{sql}");
    }
}

#[test]
fn every_blocking_statement_form_needs_a_lock_timeout() {
    for sql in [
        "ALTER TABLE IF EXISTS ONLY public.harvest_events ADD COLUMN x INT;",
        "LOCK TABLE harvest_events IN SHARE MODE;",
        "LOCK harvest_events;",
        "DROP TABLE IF EXISTS harvest_timers;",
        "TRUNCATE harvest_timers;",
        "CREATE OR REPLACE TRIGGER t BEFORE INSERT ON harvest_events FOR EACH ROW EXECUTE FUNCTION f();",
        "DROP TRIGGER IF EXISTS t ON harvest_events;",
        // A foreign key takes SHARE ROW EXCLUSIVE on the table it references.
        "CREATE TABLE harvest_new (exec_id UUID REFERENCES harvest_workflow_executions (id));",
    ] {
        let findings = lint_with_history(&[], sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}: {findings:?}");
    }
}

#[test]
fn a_lock_timeout_set_after_the_lock_does_not_count() {
    let sql = "ALTER TABLE harvest_events ADD COLUMN x INT;\n\
               SET LOCAL lock_timeout = '5s';\n";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
    assert_eq!(findings[0].line, 1, "the finding names the first lock");
}

#[test]
fn a_zero_or_default_lock_timeout_does_not_count() {
    for set in [
        "SET LOCAL lock_timeout = 0;",
        "SET LOCAL lock_timeout = '0';",
        "SET lock_timeout TO '0s';",
        "SET lock_timeout TO DEFAULT;",
        "SELECT set_config('lock_timeout', '0', true);",
    ] {
        let sql = format!("{set}\nALTER TABLE harvest_events ADD COLUMN x INT;");
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{set}: {findings:?}");
    }
}

#[test]
fn a_transaction_local_timeout_does_not_count_outside_a_transaction() {
    // `SET LOCAL` outside a transaction block only raises a warning.
    for set in [
        "SET LOCAL lock_timeout = '5s';",
        "SELECT set_config('lock_timeout', '5s', true);",
    ] {
        let sql = format!("{set}\nALTER TABLE harvest_events ADD COLUMN x INT;");
        let findings = lint_with_history(&[], &sql, false);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{set}: {findings:?}");
    }
    let session = "SET lock_timeout = '5s';\nALTER TABLE harvest_events ADD COLUMN x INT;";
    assert_eq!(lint_with_history(&[], session, false), []);
}

// ── Lexing ───────────────────────────────────────────────────────────────────

#[test]
fn comments_and_string_literals_are_not_statements() {
    let sql = "-- CREATE INDEX idx_x ON harvest_events (id);\n\
               /* ALTER TABLE harvest_events ADD COLUMN x INT; /* nested */ */\n\
               COMMENT ON TABLE harvest_schedules IS 'CREATE INDEX i ON harvest_events (id)';\n\
               SELECT E'it\\'s ALTER TABLE harvest_events', 'don''t LOCK harvest_events';\n";
    assert_eq!(lint_with_history(&[], sql, true), []);
}

#[test]
fn statements_inside_a_dollar_quoted_block_are_scanned() {
    let sql = "DO $$\nBEGIN\n    IF true THEN\n        \
               CREATE INDEX idx_x ON harvest_events (id);\n    END IF;\nEND $$;\n";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(
        rules(&findings),
        [Rule::LockTimeout, Rule::BlockingIndex],
        "{findings:?}"
    );
    assert!(findings.iter().all(|f| f.line == 4), "{findings:?}");
}

// ── concurrently-in-transaction ──────────────────────────────────────────────

#[test]
fn concurrently_needs_run_in_transaction_false() {
    let sql = "CREATE INDEX CONCURRENTLY idx_x ON harvest_events (id);";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(
        rules(&findings),
        [Rule::ConcurrentlyInTransaction],
        "{findings:?}"
    );
    assert_eq!(lint_with_history(&[], sql, false), []);
}

#[test]
fn metadata_toml_is_read_like_diesel_reads_it() {
    assert!(run_in_transaction(""));
    assert!(run_in_transaction("run_in_transaction = true\n"));
    assert!(!run_in_transaction(
        "# no transaction\nrun_in_transaction = false\n"
    ));
    assert!(!run_in_transaction(
        "run_in_transaction=false # CONCURRENTLY\n"
    ));
}

// ── The escape hatch ─────────────────────────────────────────────────────────

#[test]
fn an_annotation_above_the_statement_allows_that_rule_only() {
    let sql = "-- Prebuild this index with CONCURRENTLY first.\n\
               -- lock-safety: allow blocking-index #1810 operators prebuild it\n\
               CREATE INDEX idx_x ON harvest_events (id);\n";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");

    let both = "-- lock-safety: allow blocking-index #1810 operators prebuild it\n\
                -- lock-safety: allow lock-timeout #1810 the guard rejects a held lock\n\
                CREATE INDEX idx_x ON harvest_events (id);\n";
    assert_eq!(lint_with_history(&[], both, true), []);
}

#[test]
fn an_annotation_must_sit_directly_above_its_statement() {
    let sql = "-- lock-safety: allow lock-timeout #1810 detached by a blank line\n\
               \n\
               ALTER TABLE harvest_events ADD COLUMN x INT;\n";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(
        rules(&findings),
        [Rule::UnusedAnnotation, Rule::LockTimeout],
        "{findings:?}"
    );
}

#[test]
fn an_annotation_needs_a_known_rule_an_issue_and_a_reason() {
    for annotation in [
        "-- lock-safety: allow lock-timeout the reason has no issue",
        "-- lock-safety: allow lock-timeout #1810",
        "-- lock-safety: allow no-such-rule #1810 a reason",
        "-- lock-safety: allow unused-annotation #1810 not allowable",
        "-- lock-safety: permit lock-timeout #1810 wrong verb",
    ] {
        let sql = format!("{annotation}\nALTER TABLE harvest_events ADD COLUMN x INT;\n");
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(
            rules(&findings),
            [Rule::BadAnnotation, Rule::LockTimeout],
            "{annotation}: {findings:?}"
        );
    }
}

#[test]
fn an_annotation_that_allows_nothing_is_flagged() {
    let sql = "-- lock-safety: allow blocking-index #1810 nothing to allow here\n\
               SELECT 1;\n";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::UnusedAnnotation], "{findings:?}");
    assert_eq!(findings[0].line, 1);
}

// ── The real trees ───────────────────────────────────────────────────────────

/// RED for issue #1810: the lint flags the index rebuild in `20260915231809`.
///
/// The migration drops and rebuilds a unique index on
/// `harvest_workflow_executions` inside its transaction. It is in scope, so
/// it passes only through an explicit grandfather entry.
#[test]
fn the_20260915231809_index_rebuild_is_flagged_and_grandfathered() {
    const NAME: &str = "20260915231809_harvest_migrated_seal_terminal_at";
    let migrations = load_migrations();
    let map = real_index_tables(&migrations);
    let migration = migrations
        .iter()
        .find(|m| m.name == NAME)
        .expect("the migration issue #1810 names is on disk");
    assert!(migration.in_scope(), "{NAME} must stay inside the cutoff");

    let findings = lint(&migration.sql, migration.run_in_transaction, &map);
    let index_findings: Vec<&Finding> = findings
        .iter()
        .filter(|f| f.rule == Rule::BlockingIndex)
        .collect();
    assert_eq!(
        index_findings.len(),
        2,
        "both the DROP INDEX and the CREATE UNIQUE INDEX must be flagged: {findings:?}"
    );
    assert!(
        index_findings
            .iter()
            .all(|f| f.detail.contains("harvest_workflow_executions")),
        "{index_findings:?}"
    );
    assert!(
        rules(&findings).contains(&Rule::LockTimeout),
        "{findings:?}"
    );

    for rule in [Rule::BlockingIndex, Rule::LockTimeout] {
        assert!(
            grandfathered(NAME, rule),
            "{NAME} must be grandfathered for {}",
            rule.id()
        );
    }
}

/// The gate: no migration after the cutoff breaks a rule without an
/// annotation or a grandfather entry.
#[test]
fn migrations_after_the_cutoff_are_lock_safe() {
    let migrations = load_migrations();
    let map = real_index_tables(&migrations);
    let mut failures = Vec::new();
    for m in migrations.iter().filter(|m| m.in_scope()) {
        for f in lint(&m.sql, m.run_in_transaction, &map) {
            if !grandfathered(&m.name, f.rule) {
                failures.push(format!(
                    "{}/{}/up.sql:{}: [{}] {}",
                    m.tree,
                    m.name,
                    f.line,
                    f.rule.id(),
                    f.detail
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "these migrations take blocking locks on a hot table:\n  {}\n\
         Set `SET LOCAL lock_timeout = '5s'` before the first lock, and build \
         indexes with CONCURRENTLY. If a statement must stay, annotate it with \
         `-- {ANNOTATION_PREFIX} allow <rule> #<issue> <reason>` on the line above. \
         See docs/upgrading/online-migrations.md.",
        failures.join("\n  ")
    );
}

#[test]
fn grandfather_entries_are_shipped_in_scope_and_unique() {
    let migrations = load_migrations();
    let names: BTreeSet<&str> = migrations.iter().map(|m| m.name.as_str()).collect();
    let mut seen = BTreeSet::new();
    for (name, rule, reason) in GRANDFATHERED {
        let version = name.split('_').next().unwrap_or(name);
        assert!(names.contains(name), "{name} is not on disk");
        assert!(
            version > LOCK_SAFETY_CUTOFF,
            "{name} is out of scope, so its entry is dead"
        );
        assert!(
            version <= GRANDFATHER_CEILING,
            "{name} is newer than {GRANDFATHER_CEILING}. Use the in-file annotation."
        );
        assert!(
            rule.allowable(),
            "{name}: {} cannot be grandfathered",
            rule.id()
        );
        assert!(!reason.trim().is_empty(), "{name}: give a reason");
        assert!(seen.insert((*name, *rule)), "{name}: duplicate entry");
    }
}

#[test]
fn grandfather_entries_are_not_stale() {
    let migrations = load_migrations();
    let map = real_index_tables(&migrations);
    for (name, rule, _) in GRANDFATHERED {
        let m = migrations
            .iter()
            .find(|m| m.name == *name)
            .unwrap_or_else(|| panic!("{name} is not on disk"));
        let findings = lint(&m.sql, m.run_in_transaction, &map);
        assert!(
            rules(&findings).contains(rule),
            "{name} no longer breaks {}. Remove its GRANDFATHERED entry.",
            rule.id()
        );
    }
}

// ── CI wiring and docs ───────────────────────────────────────────────────────

/// The lint must run in the ungated `lint` job, with no `if:` condition.
#[test]
fn the_lint_runs_in_the_ci_lint_job() {
    const FILTER: &str = "--test integration migration_lock_safety::";
    let workflow = std::fs::read_to_string(workspace_root().join(".github/workflows/ci.yml"))
        .expect("ci.yml is readable")
        .replace("\r\n", "\n");
    let lint_start = workflow.find("\n  lint:").expect("ci.yml has a lint job");
    let test_start = workflow.find("\n  test:").expect("ci.yml has a test job");
    let block = &workflow[lint_start..test_start];

    let at = block
        .find(FILTER)
        .unwrap_or_else(|| panic!("the lint job must run `{FILTER}`"));
    let step_start = block[..at]
        .rfind("\n      - ")
        .expect("the run line is in a step");
    let step_end = block[at..]
        .find("\n      - ")
        .map_or(block.len(), |end| at + end);
    let stanza = &block[step_start..step_end];
    assert!(
        !stanza.contains("\n        if:"),
        "the lock-safety step must run unconditionally:\n{stanza}"
    );
}

/// The author guide names every hot table and every rule the lint enforces.
#[test]
fn the_author_guide_matches_the_lint() {
    let path = workspace_root().join("docs/upgrading/online-migrations.md");
    let guide =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    for table in HOT_TABLES {
        assert!(
            guide.contains(&format!("`{table}`")),
            "the guide must list {table}"
        );
    }
    for rule in Rule::ALL {
        assert!(
            guide.contains(&format!("`{}`", rule.id())),
            "the guide must explain {}",
            rule.id()
        );
    }
    for needle in [
        "-- lock-safety: allow",
        "run_in_transaction = false",
        LOCK_SAFETY_CUTOFF,
    ] {
        assert!(guide.contains(needle), "the guide must mention {needle}");
    }
}

/// Every SQL example in the author guide passes the lint.
///
/// An example that runs `CONCURRENTLY` is linted with
/// `run_in_transaction = false`, as the guide tells authors to write it. The
/// annotation template is skipped.
#[test]
fn the_author_guide_examples_pass_the_lint() {
    let path = workspace_root().join("docs/upgrading/online-migrations.md");
    let guide =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let examples: Vec<&str> = guide
        .split("```sql\n")
        .skip(1)
        .filter_map(|rest| rest.split_once("```").map(|(body, _)| body))
        .filter(|body| !body.contains("<rule>"))
        .collect();
    assert!(examples.len() >= 3, "the guide lost its SQL examples");
    for example in examples {
        let concurrent = tokenize(example)
            .0
            .iter()
            .any(|t| t.tok == Tok::Word("concurrently".to_string()));
        let findings = lint_with_history(&[], example, !concurrent);
        assert_eq!(
            findings,
            [],
            "this guide example fails the lint:\n{example}"
        );
    }
}
