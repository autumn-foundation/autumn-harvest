//! Online-migration lock-safety lint (issue #1810). No DB, no feature gate.
//!
//! A migration that locks a hot table stops the engine for as long as it
//! holds or waits for that lock. This lint reads each `up.sql` after
//! `LOCK_SAFETY_CUTOFF` and enforces two rules on the hot tables:
//!
//!   1. `lock-timeout`: a statement that takes a blocking lock on a hot table
//!      needs a non-zero `lock_timeout` in force. Without one, the statement
//!      waits behind a long transaction, and every later query queues behind it.
//!   2. `blocking-index`: `CREATE INDEX`, `DROP INDEX` and `REINDEX` on a hot
//!      table must use `CONCURRENTLY`. A plain build blocks writes until it
//!      ends. A plain drop needs `ACCESS EXCLUSIVE`, which queues all access.
//!
//! An in-file annotation is the reviewed escape hatch. Shipped migrations
//! cannot change, so they are grandfathered by name in `GRANDFATHERED`.
//! `docs/upgrading/online-migrations.md` is the author guide.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use autumn_harvest::partition::{LEGACY_PARTITION, PARTITION_PREFIX};

use super::ci_run_coverage::{NO_FULL_RUN_FLAGS, SHELL_OPERATORS, parse_workflow, ungated};

/// The last migration this lint does not bind, inclusive.
///
/// Issue #1810 names `20260915231809` as the first offender. That migration
/// must stay in scope, so the cutoff is the migration just before it.
const LOCK_SAFETY_CUTOFF: &str = "20260914165542";

/// Each migration at or before `LOCK_SAFETY_CUTOFF` that was on disk when the
/// lint landed, as `tree/name`.
///
/// Only these are exempt. A new migration with an old version is still
/// linted, so a backdated name cannot skip the lint.
const LEGACY_MIGRATIONS: &str = include_str!("lock_safety_legacy.txt");

/// The `digest` of `LEGACY_MIGRATIONS`. The list is frozen: a swapped entry
/// changes the digest even when the size stays the same.
const LEGACY_MIGRATION_DIGEST: u64 = 0x1dc3_f33d_ee51_d5ed;

/// The newest migration that `GRANDFATHERED` may name.
///
/// This is the newest migration on disk when the lint landed. A newer
/// migration cannot be grandfathered, so it uses the in-file annotation.
const GRANDFATHER_CEILING: &str = "20261003201739";

/// Tables that the engine reads or writes on every claim or workflow step.
///
/// A blocking lock on one of them stalls the engine, not one feature. Each
/// operator action writes `harvest_audit_log`. `harvest_workflow_outbox` lives
/// in the application database, and the application writes it in its own
/// transactions.
const HOT_TABLES: &[&str] = &[
    "harvest_activity_pauses",
    "harvest_audit_log",
    "harvest_events",
    "harvest_queue_pauses",
    "harvest_rate_limit_buckets",
    "harvest_shard_generation",
    "harvest_signals",
    "harvest_task_queue",
    "harvest_timers",
    "harvest_workers",
    "harvest_workflow_executions",
    "harvest_workflow_outbox",
];

/// Hot tables that the opt-in partitioned layout turns into partitioned parents.
///
/// Postgres does not build or drop an index concurrently on a partitioned
/// parent, so `CONCURRENTLY` is no fix for these tables.
const PARTITIONED_TABLES: &[&str] = &["harvest_events"];

/// Migration trees, relative to the workspace root.
///
/// The plugin `harvest` tree runs against the same database as the core tree.
/// The `app` tree runs against the application database.
const MIGRATION_TREES: &[&str] = &[
    "autumn-harvest/migrations",
    "autumn-harvest-plugin/migrations/harvest",
    APP_TREE,
];

/// The one tree that targets the application database, not the Harvest one.
const APP_TREE: &str = "autumn-harvest-plugin/migrations/app";

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
        "20261001190405_harvest_audit_unexported_idx_lazy",
        Rule::BlockingIndex,
        "Drops the audit claim-scan index through EXECUTE, only on a database \
         with no export cursor. Shipped before this lint.",
    ),
    (
        "20261001190405_harvest_audit_unexported_idx_lazy",
        Rule::LockTimeout,
        "The 5 s bound and the drop sit in one IF branch, so the bound is in \
         force. The lint does not count a setter inside a branch.",
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
    (
        "20261003201739_harvest_task_queue_hygiene",
        Rule::BlockingIndex,
        "Drops three redundant task-queue indexes without CONCURRENTLY, after \
         a guarded check that the replacement index is valid. Shipped before \
         this lint.",
    ),
    (
        "20261003201739_harvest_task_queue_hygiene",
        Rule::LockTimeout,
        "The index drops and the guarded EXECUTE build have no lock bound. \
         Shipped before this lint.",
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
    /// The token index where the statement starts. An annotation allows one
    /// statement, so it binds to this.
    stmt: usize,
    detail: String,
}

/// What earlier migrations created, as far as the lint needs to know.
#[derive(Clone, Debug, Default)]
struct History {
    /// Each index, mapped to the tables it may sit on.
    /// The key comes from `index_key`, so it holds the schema. The value holds
    /// every table that a build of that name has named, so a later build cannot
    /// hide a hot table.
    indexes: BTreeMap<String, BTreeSet<String>>,
    /// Each table, mapped to the tables that its foreign keys reference.
    references: BTreeMap<String, BTreeSet<String>>,
    /// Each partition of a hot table, without its schema. A partition takes
    /// the live writes of its parent, so it is hot too.
    partitions: BTreeSet<String>,
    /// Each table that a migration created with `PARTITION BY`, without its
    /// schema. Postgres runs no `CONCURRENTLY` index DDL on such a table.
    partitioned: BTreeSet<String>,
    /// Each routine that may clear the bound, without its schema. A call of
    /// such a routine in a later migration clears the bound too.
    clearing_routines: BTreeSet<String>,
    /// Each routine in a language that the lint cannot read, without its
    /// schema. A call of such a routine may lock any table.
    foreign_routines: BTreeSet<String>,
    /// Whether an earlier migration may have changed `search_path`. A session
    /// value outlives its file, so a later file starts after the change.
    search_path_changed: bool,
    /// Whether an earlier migration left `standard_conforming_strings` off.
    nonstandard_strings: bool,
    /// Each routine whose body may take a lock, without its schema. A call of
    /// such a routine in a later migration counts as a lock.
    locking_routines: BTreeSet<String>,
    /// The full identity of each routine that may lock and does not clear the
    /// bound, from `identity`. Only a call of that exact identity keeps an
    /// outside bound. Another schema or arity may reach a routine that clears.
    bounded_routines: BTreeSet<String>,
    /// The full identity of each routine that bounds every lock it takes, from
    /// `identity`. Postgres applies that bound on each call. So a call of that
    /// exact identity needs no outside bound.
    self_bounded_routines: BTreeSet<String>,
    /// Each routine name, without its schema, that a definition or an `ALTER`
    /// may leave with an unbounded lock. No identity of such a name counts as
    /// self-bounded, because a later call may reach that version.
    unbounded_routines: BTreeSet<String>,
    /// Each routine whose body may change `search_path`, without its schema.
    /// A call of such a routine in a later migration changes the path too.
    path_routines: BTreeSet<String>,
}

impl History {
    /// Whether `table` is hot: a listed table, or a partition of a hot table.
    ///
    /// The partition manager creates `harvest_events` partitions at run time,
    /// so no migration names them. Their names come from `partition.rs`.
    fn is_hot(&self, table: &str) -> bool {
        let name = base(table);
        HOT_TABLES.contains(&name)
            || self.partitions.contains(name)
            || name.starts_with(PARTITION_PREFIX)
            || name == LEGACY_PARTITION
    }

    /// The table of index `name`. A hot table wins when the name is ambiguous.
    fn index_table(&self, name: &str) -> Option<String> {
        let tables = self.indexes.get(&index_key(name, name))?;
        tables
            .iter()
            .find(|t| self.is_hot(t))
            .or_else(|| tables.iter().next())
            .cloned()
    }

    /// Whether `table` is a partitioned table, which rejects `CONCURRENTLY`.
    fn is_partitioned(&self, table: &str) -> bool {
        PARTITIONED_TABLES.contains(&base(table)) || self.partitioned.contains(base(table))
    }

    /// Forget every cold index on `table`.
    ///
    /// The match ignores the schema, so it may forget too much. A forgotten
    /// index is unknown, and an unknown index counts as hot. A hot mapping
    /// stays, because it can only make a later drop stricter.
    fn forget_table(&mut self, table: &str) {
        let hot = self.is_hot(table);
        for tables in self.indexes.values_mut() {
            tables.retain(|t| hot || base(t) != base(table));
        }
        self.indexes.retain(|_, tables| !tables.is_empty());
    }

    /// Forget every index that no hot table may own.
    fn forget_cold_indexes(&mut self) {
        let keep: BTreeSet<String> = self
            .indexes
            .iter()
            .filter(|(_, tables)| tables.iter().any(|t| self.is_hot(t)))
            .map(|(key, _)| key.clone())
            .collect();
        self.indexes.retain(|key, _| keep.contains(key));
    }

    /// Build the history that `migrations` leave behind, in order.
    fn of<'a>(migrations: impl IntoIterator<Item = &'a str>) -> Self {
        let mut history = Self::default();
        for sql in migrations {
            analyse(sql, &mut history);
        }
        history
    }
}

/// Lint one `up.sql` against the history of the migrations before it.
///
/// A `DROP INDEX` does not name its table, so the lint reads the table from
/// the history. A definition later in the same file does not count.
fn lint(sql: &str, run_in_transaction: bool, history: &History) -> Vec<Finding> {
    let analysis = analyse(sql, &mut history.clone());
    let mut findings = Vec::new();

    for hit in &analysis.hits {
        let Kind::Index { concurrent } = hit.kind else {
            continue;
        };
        if concurrent {
            let reason = if hit.in_body {
                Some("cannot run inside a DO block or a function")
            } else if run_in_transaction {
                Some(
                    "cannot run in a transaction. Set `run_in_transaction = false` in metadata.toml",
                )
            } else if analysis.statement_count > 1 {
                Some(
                    "must be the only statement in its file. Diesel sends the file as one \
                     batch, and Postgres runs a batch as one transaction",
                )
            } else {
                None
            };
            if let Some(reason) = reason {
                findings.push(Finding {
                    rule: Rule::ConcurrentlyInTransaction,
                    line: hit.line,
                    stmt: hit.at,
                    detail: format!("{} CONCURRENTLY {reason}.", hit.verb),
                });
            }
            // An unknown table may be the partitioned parent, so it fails closed.
            // `REINDEX CONCURRENTLY` does run on a partitioned table.
            let partitioned = hit.verb != "REINDEX"
                && hit
                    .table
                    .as_deref()
                    .is_none_or(|t| history.is_partitioned(t));
            if partitioned {
                findings.push(Finding {
                    rule: Rule::BlockingIndex,
                    line: hit.line,
                    stmt: hit.at,
                    detail: format!(
                        "{} CONCURRENTLY on {} fails on the partitioned layout, because \
                         Postgres does not run it on a partitioned parent. Use the \
                         per-partition recipe, then annotate the statement.",
                        hit.label(),
                        hit.table_name()
                    ),
                });
            }
        } else if hit.hot {
            findings.push(Finding {
                rule: Rule::BlockingIndex,
                line: hit.line,
                stmt: hit.at,
                detail: format!(
                    "plain {} on {} {}",
                    hit.label(),
                    hit.table_name(),
                    index_cost(hit.verb)
                ),
            });
        }
    }

    // Every hot lock needs a bound in force. Report each statement without
    // one, so an annotation on one lock never covers another.
    let mut reported = BTreeSet::new();
    let unbounded = analysis
        .hits
        .iter()
        .filter(|hit| needs_bound(&analysis, hit))
        .filter(|hit| reported.insert(hit.at));
    for lock in unbounded {
        findings.push(Finding {
            rule: Rule::LockTimeout,
            line: lock.line,
            stmt: lock.at,
            detail: format!(
                "{} locks {} with no non-zero lock_timeout in force",
                lock.label(),
                lock.table_name()
            ),
        });
    }

    apply_annotations(sql, &analysis.comments, findings)
}

/// What a plain form of an index statement costs a hot table.
fn index_cost(verb: &str) -> &'static str {
    match verb {
        "CREATE INDEX" => "holds SHARE, which blocks writes, for the whole build",
        "DROP INDEX" => "needs ACCESS EXCLUSIVE, which queues every read and write",
        "REINDEX" => "blocks writes for the whole rebuild",
        _ => "builds an index under ACCESS EXCLUSIVE",
    }
}

/// Read the gap after a string literal at `i`. Return its newline count and
/// the next index.
///
/// A literal continues only across a gap with a newline. Postgres counts a
/// `--` comment in the gap as whitespace, but not a `/* */` comment.
fn continuation_gap(chars: &[char], mut i: usize) -> Option<(usize, usize)> {
    // `newlines` counts `\n` for line numbers. Postgres also reads a lone
    // `\r` as a newline.
    let mut newlines = 0;
    let mut newline = false;
    loop {
        match chars.get(i) {
            Some('\n') => {
                newlines += 1;
                newline = true;
                i += 1;
            }
            Some('\r') => {
                newline = true;
                i += 1;
            }
            Some(c) if c.is_whitespace() => i += 1,
            Some('-') if chars.get(i + 1) == Some(&'-') => i = line_comment_end(chars, i),
            _ => break,
        }
    }
    newline.then_some((newlines, i))
}

/// The index of the newline that ends the `--` comment at `i`, or the end.
///
/// Postgres ends a line comment at `\n` or `\r`.
fn line_comment_end(chars: &[char], i: usize) -> usize {
    (i..chars.len())
        .find(|&j| matches!(chars[j], '\n' | '\r'))
        .unwrap_or(chars.len())
}

/// Whether a bound is in force at token `at`.
///
/// A local value holds until the transaction ends. A session value outlives
/// a commit, but a rollback restores the value from before the transaction.
///
/// Diesel sends the file as one batch, and the batch is always in a
/// transaction. After a `COMMIT` or `ROLLBACK`, the next statement opens a new
/// implicit block. A later `BEGIN` takes over that block and starts nothing
/// new, so it needs no case of its own.
fn timeout_in_force(timeouts: &[(usize, Timeout)], at: usize) -> bool {
    let mut session = false;
    let mut local = None;
    // The session value when the current transaction began.
    let mut saved = false;
    for (_, change) in timeouts.iter().take_while(|(k, _)| *k < at) {
        match *change {
            Timeout::Set {
                bounds,
                local: true,
            } => local = Some(bounds),
            Timeout::Set {
                bounds,
                local: false,
            } => {
                session = bounds;
                local = None;
            }
            Timeout::Commit => {
                saved = session;
                local = None;
            }
            Timeout::Rollback => {
                session = saved;
                local = None;
            }
            Timeout::MaybeCommit => {
                saved = saved && session;
                // A local value holds if the commit does not run.
                local = local.map(|bounds| bounds && session);
            }
            Timeout::RollbackToSavepoint => {
                session = false;
                local = None;
            }
        }
    }
    local.unwrap_or(session)
}

/// Whether `hit` locks a hot table with no bound in force.
fn needs_bound(analysis: &Analysis, hit: &Hit) -> bool {
    hit.hot && hit.kind != (Kind::Index { concurrent: true }) && !bound_in_force(analysis, hit)
}

/// Whether a bound is in force for `hit`.
///
/// A lock in a function body runs when something calls the function, maybe
/// long after the migration. Only a bound set earlier in the same body holds.
/// A lock that unreadable `EXECUTE` SQL takes never counts as bounded.
fn bound_in_force(analysis: &Analysis, hit: &Hit) -> bool {
    // The hidden code may clear the bound before it locks.
    if [
        UNREADABLE_EXECUTE,
        FOREIGN_CODE,
        NONSTANDARD_STRINGS,
        ROUTINE_RESET,
        UNREAD_CLEARING_CALL,
    ]
    .contains(&hit.verb)
    {
        return false;
    }
    let Some(from) = hit.body_start else {
        return timeout_in_force(&analysis.timeouts, hit.at);
    };
    // A change in a nested routine body belongs to that routine, not to this one.
    let owner = |at: usize| {
        analysis
            .bodies
            .iter()
            .filter(|&&(start, end)| start <= at && at < end)
            .map(|&(start, _)| start)
            .max()
    };
    let mine = owner(hit.at);
    let local: Vec<(usize, Timeout)> = analysis
        .body_timeouts
        .iter()
        .filter(|(k, _)| *k >= from && owner(*k) == mine)
        .copied()
        .collect();
    // A routine setting sits at the first body token and applies before the
    // body runs. So it covers a lock at that token too.
    timeout_in_force(&local, hit.at.max(from + 1))
}

/// One change to the session's `lock_timeout`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Timeout {
    /// A new value. `local` holds for the current transaction only.
    Set { bounds: bool, local: bool },
    /// `COMMIT` or `END`, which drops every local value.
    Commit,
    /// `ROLLBACK` or `ABORT`, which also undoes every session change since
    /// the transaction began.
    Rollback,
    /// `ROLLBACK TO SAVEPOINT`. The lint does not track savepoints, so it
    /// assumes no bound remains.
    RollbackToSavepoint,
    /// A `COMMIT` that may not run. It may drop every local value. The saved
    /// value keeps a bound only when it held both before and after. A local
    /// value keeps a bound only when the session value bounds too.
    MaybeCommit,
}

// ── Annotations ──────────────────────────────────────────────────────────────

/// A valid allow annotation.
struct Annotation {
    rule: Rule,
    line: usize,
    /// The statement this annotation allows, once a finding has used it.
    bound: Option<usize>,
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
                bound: None,
            }),
            Some(Err(detail)) => out.push(Finding {
                rule: Rule::BadAnnotation,
                line: comment.line,
                stmt: 0,
                detail,
            }),
        }
    }

    for finding in findings {
        let mut allowed = false;
        let mut line = finding.line;
        while line > 1 && comment_lines.contains(&(line - 1)) {
            line -= 1;
            let free = annotations.iter_mut().find(|a| {
                a.line == line
                    && a.rule == finding.rule
                    && a.bound.is_none_or(|stmt| stmt == finding.stmt)
            });
            if let Some(annotation) = free {
                annotation.bound = Some(finding.stmt);
                allowed = true;
                break;
            }
        }
        if !allowed {
            out.push(finding);
        }
    }

    out.extend(
        annotations
            .iter()
            .filter(|a| a.bound.is_none())
            .map(|a| Finding {
                rule: Rule::UnusedAnnotation,
                line: a.line,
                stmt: 0,
                detail: format!(
                    "allows {} but the statement below needs no such allowance",
                    a.rule.id()
                ),
            }),
    );
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
    /// How many dollar-quoted bodies enclose the token. Zero is top level.
    depth: usize,
    /// Whether the token runs when the migration runs. A function body does
    /// not, because it runs only when something calls the function.
    runs: bool,
    /// Whether the token is a quoted identifier, which is never a keyword.
    quoted: bool,
}

/// A `--` comment, without the dashes.
struct Comment {
    text: String,
    line: usize,
}

/// Split `sql` into tokens and `--` comments.
///
/// Comments and string literals never become words, so prose cannot match a
/// statement. A dollar-quoted body is lexed on its own and scanned as code,
/// because a `DO $$` block holds real DDL. Lexer state never leaks past the
/// closing delimiter.
fn tokenize(sql: &str) -> (Vec<Token>, Vec<Comment>) {
    let chars: Vec<char> = sql.chars().collect();
    let mut toks = Vec::new();
    let mut comments = Vec::new();
    lex(&chars, 1, 0, true, &mut toks, &mut comments);
    mark_atomic_bodies(&mut toks);
    (toks, comments)
}

/// Mark each unquoted `BEGIN ATOMIC` function body as code that does not run.
///
/// `CREATE FUNCTION` stores the body and runs nothing in it, as with a
/// quoted body. The body ends at the `END` that no SQL `CASE` claims.
fn mark_atomic_bodies(toks: &mut [Token]) {
    // A quoted name such as `"end"` is never a keyword.
    let word = |t: &Token, w: &str| t.tok == Tok::Word(w.to_string()) && !t.quoted;
    let mut k = 0;
    while k + 1 < toks.len() {
        let depth = toks[k].depth;
        let in_create = statement_head(&toks[..k], depth).is_some_and(|t| word(t, "create"));
        // A SQL function may have a `RETURN expression` body instead. It runs
        // on each call, up to the end of the statement.
        if in_create && word(&toks[k], "return") {
            let end = (k..toks.len())
                .find(|&j| toks[j].depth == depth && toks[j].tok == Tok::Punct(';'))
                .unwrap_or(toks.len());
            for tok in &mut toks[k..end] {
                tok.runs = false;
            }
            k = end.max(k + 1);
            continue;
        }
        let opens = in_create && word(&toks[k], "begin") && word(&toks[k + 1], "atomic");
        if !opens {
            k += 1;
            continue;
        }
        let mut cases = 0_usize;
        let mut end = toks.len() - 1;
        for (j, tok) in toks.iter().enumerate().skip(k + 2) {
            if tok.depth != depth {
                continue;
            }
            if word(tok, "case") {
                cases += 1;
            } else if word(tok, "end") {
                if cases == 0 {
                    end = j;
                    break;
                }
                cases -= 1;
            }
        }
        for tok in &mut toks[k..=end] {
            tok.runs = false;
        }
        k = end + 1;
    }
}

/// Lex `chars`, which start on line `line` inside `depth` dollar bodies.
///
/// `runs` says whether these tokens run when the migration runs.
#[allow(clippy::too_many_lines)]
fn lex(
    chars: &[char],
    mut line: usize,
    depth: usize,
    runs: bool,
    toks: &mut Vec<Token>,
    comments: &mut Vec<Comment>,
) {
    let at = |i: usize| chars.get(i).copied();
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
            i = line_comment_end(chars, i);
            comments.push(Comment {
                text: chars[start..i].iter().collect(),
                line,
            });
        } else if c == '/' && next == Some('*') {
            // Block comments nest in Postgres.
            let mut nesting = 0;
            while let Some(c) = at(i) {
                if c == '/' && at(i + 1) == Some('*') {
                    nesting += 1;
                    i += 2;
                } else if c == '*' && at(i + 1) == Some('/') {
                    nesting -= 1;
                    i += 2;
                    if nesting == 0 {
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
            // Postgres joins literals that only whitespace with a newline
            // parts, as in `'a'` and then `'b'` on the next line. Only a bare
            // quote continues a literal, and the joined literal keeps the
            // escape mode of its first part.
            'literal: loop {
                while let Some(c) = at(i) {
                    line += usize::from(c == '\n');
                    if escapes && c == '\\' {
                        if let Some((decoded, len)) = e_escape(&chars[i + 1..]) {
                            line += usize::from(at(i + 1) == Some('\n'));
                            value.push(decoded);
                            i += 1 + len;
                        } else {
                            i += 1;
                        }
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
                match continuation_gap(chars, i) {
                    Some((newlines, next)) if at(next) == Some('\'') => {
                        line += newlines;
                        i = next + 1;
                    }
                    _ => break 'literal,
                }
            }
            // A `DO` body may be a plain string, so it is code too. So is the
            // SQL that PL/pgSQL `EXECUTE` runs.
            if in_do_statement(toks, depth) {
                let body: Vec<char> = value.chars().collect();
                lex(&body, start_line, depth + 1, runs, toks, comments);
            } else if in_execute_statement(toks, depth) {
                lex_execute_sql(&value, start_line, depth, runs, toks, comments);
            } else if function_body_follows(toks, depth) {
                // A function body runs later, so it does not run now.
                let body: Vec<char> = value.chars().collect();
                lex(&body, start_line, depth + 1, false, toks, comments);
            } else {
                toks.push(Token {
                    tok: Tok::Str(value),
                    line: start_line,
                    depth,
                    runs,
                    quoted: false,
                });
            }
        } else if c == '"' {
            // A quoted identifier keeps its case. `""` is an escaped quote.
            let start_line = line;
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
                    value.push(if c == '.' { QUOTED_DOT } else { c });
                    i += 1;
                }
            }
            toks.push(Token {
                tok: Tok::Word(value),
                line: start_line,
                depth,
                runs,
                quoted: true,
            });
        } else if let Some(len) = dollar_tag_len(&chars[i..]) {
            // Find the matching close first, then lex only the body.
            let tag = &chars[i..i + len];
            let body_start = i + len;
            let body_end = (body_start..chars.len())
                .find(|&j| chars[j..].starts_with(tag))
                .unwrap_or(chars.len());
            let body = &chars[body_start..body_end];
            // Only a `DO` body runs now. Any other body, such as a
            // function body or a string, runs later or never.
            // A `DO` body and the SQL that `EXECUTE` runs are code that runs
            // now. A function body after `AS` is code that runs later. Any
            // other dollar body is a string, such as a `set_config` argument.
            let after = |word: &str| {
                toks.iter()
                    .rev()
                    .find(|t| t.depth == depth)
                    .is_some_and(|t| is_keyword(t, word))
            };
            if in_execute_statement(toks, depth) || (depth > 0 && after("execute")) {
                let sql: String = body.iter().collect();
                lex_execute_sql(&sql, line, depth, runs, toks, comments);
            } else if in_do_statement(toks, depth) || after("as") {
                let body_runs = runs && in_do_statement(toks, depth);
                lex(body, line, depth + 1, body_runs, toks, comments);
            } else {
                toks.push(Token {
                    tok: Tok::Str(body.iter().collect()),
                    line,
                    depth,
                    runs,
                    quoted: false,
                });
            }
            line += body.iter().filter(|c| **c == '\n').count();
            i = (body_end + len).min(chars.len());
        } else if matches!(c, 'u' | 'U')
            && next == Some('&')
            && matches!(at(i + 2), Some('"' | '\''))
        {
            // `U&"..."` and `U&'...'` take Unicode escapes.
            let quote = chars[i + 2];
            let start_line = line;
            let (mut raw, end) = quoted(chars, i + 3, quote);
            line += raw.matches('\n').count();
            i = end;
            // A string continues across whitespace with a newline, before
            // its escapes are decoded.
            while quote == '\''
                && let Some((newlines, next)) = continuation_gap(chars, i)
                && at(next) == Some('\'')
            {
                let (more, end) = quoted(chars, next + 1, quote);
                line += newlines + more.matches('\n').count();
                raw.push_str(&more);
                i = end;
            }
            let escape = uescape(chars, &mut i, &mut line).unwrap_or('\\');
            let value = decode_unicode(&raw, escape);
            if quote == '\'' && in_do_statement(toks, depth) {
                let body: Vec<char> = value.chars().collect();
                lex(&body, start_line, depth + 1, runs, toks, comments);
                continue;
            }
            if quote == '\'' && in_execute_statement(toks, depth) {
                lex_execute_sql(&value, start_line, depth, runs, toks, comments);
                continue;
            }
            if quote == '\'' && function_body_follows(toks, depth) {
                let body: Vec<char> = value.chars().collect();
                lex(&body, start_line, depth + 1, false, toks, comments);
                continue;
            }
            toks.push(Token {
                tok: if quote == '"' {
                    Tok::Word(value.replace('.', &QUOTED_DOT.to_string()))
                } else {
                    Tok::Str(value)
                },
                line: start_line,
                depth,
                runs,
                quoted: quote == '"',
            });
        } else if c == LITERAL_PLACEHOLDER {
            // A `%L` value outside a string is code that the lint cannot read.
            toks.push(Token {
                tok: Tok::Word("%s".to_string()),
                line,
                depth,
                runs,
                quoted: true,
            });
            i += 1;
        } else if is_ident(c) {
            let start = i;
            while at(i).is_some_and(|c| is_ident(c) || c == '$') {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            toks.push(Token {
                tok: Tok::Word(word.to_lowercase()),
                line,
                depth,
                runs,
                quoted: false,
            });
        } else {
            toks.push(Token {
                tok: Tok::Punct(c),
                line,
                depth,
                runs,
                quoted: false,
            });
            i += 1;
        }
    }
}

/// Decode one `E''` escape. `rest` starts after the backslash.
///
/// Returns the character and the length of the escape. A whitespace escape
/// must still split words in a `DO` body, so `\b` and `\f` become a space.
fn e_escape(rest: &[char]) -> Option<(char, usize)> {
    // `max` digits at most, `exact` when the escape needs all of them.
    let number = |from: usize, max: usize, radix: u32, exact: bool| {
        let tail = rest.get(from..)?;
        let len = tail
            .iter()
            .take(max)
            .take_while(|c| c.is_digit(radix))
            .count();
        if len == 0 || (exact && len < max) {
            return None;
        }
        let text: String = tail[..len].iter().collect();
        let decoded = char::from_u32(u32::from_str_radix(&text, radix).ok()?)?;
        Some((decoded, from + len))
    };
    let first = *rest.first()?;
    let simple = match first {
        'n' => '\n',
        't' => '\t',
        'r' => '\r',
        'b' | 'f' => ' ',
        'x' => return number(1, 2, 16, false).or(Some(('x', 1))),
        'u' => return number(1, 4, 16, true).or(Some(('u', 1))),
        'U' => return number(1, 8, 16, true).or(Some(('U', 1))),
        '0'..='7' => return number(0, 3, 8, false),
        other => other,
    };
    Some((simple, 1))
}

/// The body of a quoted token that opens before `from`, and the index after
/// its closing quote. A doubled quote is one quote.
fn quoted(chars: &[char], from: usize, quote: char) -> (String, usize) {
    let mut value = String::new();
    let mut i = from;
    while let Some(&c) = chars.get(i) {
        if c == quote && chars.get(i + 1) == Some(&quote) {
            value.push(quote);
            i += 2;
        } else if c == quote {
            return (value, i + 1);
        } else {
            value.push(c);
            i += 1;
        }
    }
    (value, i)
}

/// The escape character of a `UESCAPE 'c'` clause at `*i`, if there is one.
///
/// The clause is consumed only when it is there.
fn uescape(chars: &[char], i: &mut usize, line: &mut usize) -> Option<char> {
    let mut j = skip_blank(chars, *i);
    let word: String = chars.get(j..j + 7)?.iter().collect();
    if !word.eq_ignore_ascii_case("uescape") {
        return None;
    }
    j = skip_blank(chars, j + 7);
    let (Some('\''), Some(&escape), Some('\'')) =
        (chars.get(j), chars.get(j + 1), chars.get(j + 2))
    else {
        return None;
    };
    *line += chars[*i..j].iter().filter(|c| **c == '\n').count();
    *i = j + 3;
    Some(escape)
}

/// The index of the first character at or after `i` that is not whitespace
/// or a comment. Postgres reads a comment as whitespace. A `/* */` comment
/// may nest.
fn skip_blank(chars: &[char], mut i: usize) -> usize {
    loop {
        match (chars.get(i), chars.get(i + 1)) {
            (Some(c), _) if c.is_whitespace() => i += 1,
            (Some('-'), Some('-')) => i = line_comment_end(chars, i),
            (Some('/'), Some('*')) => {
                let mut depth = 0_usize;
                while i < chars.len() {
                    if chars[i] == '/' && chars.get(i + 1) == Some(&'*') {
                        depth += 1;
                        i += 2;
                    } else if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
                        depth -= 1;
                        i += 2;
                        if depth == 0 {
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            _ => return i,
        }
    }
}

/// Decode the `\XXXX` and `\+XXXXXX` escapes of a `U&` token.
///
/// `escape` replaces the backslash. A doubled escape is one escape. A bad
/// escape stays as it is.
fn decode_unicode(raw: &str, escape: char) -> String {
    let chars: Vec<char> = raw.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while let Some(&c) = chars.get(i) {
        if c != escape {
            out.push(c);
            i += 1;
            continue;
        }
        if chars.get(i + 1) == Some(&escape) {
            out.push(escape);
            i += 2;
            continue;
        }
        let (from, len) = if chars.get(i + 1) == Some(&'+') {
            (i + 2, 6)
        } else {
            (i + 1, 4)
        };
        let decoded = chars
            .get(from..from + len)
            .map(|hex| hex.iter().collect::<String>())
            .and_then(|hex| u32::from_str_radix(&hex, 16).ok())
            .and_then(char::from_u32);
        if let Some(d) = decoded {
            out.push(d);
            i = from + len;
        } else {
            out.push(c);
            i += 1;
        }
    }
    out
}

/// Whether the open statement at `depth` starts with `DO`.
fn in_do_statement(toks: &[Token], depth: usize) -> bool {
    statement_head(toks, depth).is_some_and(|t| is_keyword(t, "do"))
}

/// Whether `t` is the unquoted keyword `w`. A quoted name is never a keyword.
fn is_keyword(t: &Token, w: &str) -> bool {
    !t.quoted && t.tok == Tok::Word(w.to_string())
}

/// Whether the open statement at `depth` holds a PL/pgSQL `EXECUTE`.
///
/// `FOR ... IN EXECUTE`, `RETURN QUERY EXECUTE` and `OPEN ... FOR EXECUTE`
/// run dynamic SQL too. The `EXECUTE FUNCTION` of a trigger does not. A
/// top-level `EXECUTE` runs a prepared statement, so its arguments are data.
fn in_execute_statement(toks: &[Token], depth: usize) -> bool {
    // The open statement, last token first. So `open[i + 1]` comes before
    // `open[i]`, and `open[i - 1]` comes after it.
    let open = open_statement(toks, depth);
    let dynamic = |i: usize| {
        let before = open.get(i + 1);
        let after = i.checked_sub(1).map(|j| open[j]);
        let opens = before.is_none_or(|t| ["in", "query", "for"].iter().any(|w| is_keyword(t, w)));
        let routine =
            after.is_some_and(|t| is_keyword(t, "function") || is_keyword(t, "procedure"));
        let assigned = after.is_some_and(|t| matches!(t.tok, Tok::Punct(':' | '=')));
        is_keyword(open[i], "execute") && opens && !routine && !assigned
    };
    depth > 0 && (0..open.len()).any(dynamic)
}

/// Whether the next token at `depth` is the body of a `CREATE FUNCTION` or
/// `CREATE PROCEDURE`: the statement starts with `CREATE`, and `AS` is the
/// last token.
fn function_body_follows(toks: &[Token], depth: usize) -> bool {
    toks.iter()
        .rev()
        .find(|t| t.depth == depth)
        .is_some_and(|t| is_keyword(t, "as"))
        && statement_head(toks, depth).is_some_and(|t| is_keyword(t, "create"))
}

/// The first token of the open statement at `depth`.
///
/// Inside a PL/pgSQL body, `BEGIN`, `THEN`, `ELSE` and `LOOP` also end the
/// statement before, as in `Stmts::new`.
fn statement_head(toks: &[Token], depth: usize) -> Option<&Token> {
    open_statement(toks, depth).last().copied()
}

/// The tokens of the open statement at `depth`, last token first.
fn open_statement(toks: &[Token], depth: usize) -> Vec<&Token> {
    let opens = |t: &Token| match &t.tok {
        Tok::Punct(';') => true,
        Tok::Word(w) => {
            depth > 0 && !t.quoted && ["begin", "then", "else", "loop"].contains(&w.as_str())
        }
        _ => false,
    };
    toks.iter()
        .rev()
        .filter(|t| t.depth == depth)
        .take_while(|t| !opens(t))
        .collect()
}

/// Lex the SQL that a PL/pgSQL `EXECUTE` at `depth` runs.
///
/// `format()` substitutes `%s` without regard to quotes or comments. So a
/// `%s` anywhere in the text adds the `%s` marker after the SQL, which makes
/// the `EXECUTE` unreadable.
fn lex_execute_sql(
    sql: &str,
    line: usize,
    depth: usize,
    runs: bool,
    toks: &mut Vec<Token>,
    comments: &mut Vec<Comment>,
) {
    let (filled, text_placeholder) = fill_placeholders(sql);
    let body: Vec<char> = filled.chars().collect();
    lex(&body, line, depth + 1, runs, toks, comments);
    if text_placeholder {
        toks.push(Token {
            tok: Tok::Word("%s".to_string()),
            line,
            depth: depth + 1,
            runs,
            quoted: true,
        });
    }
}

/// Replace each `format()` placeholder in `sql` with an unknown value. Also
/// return whether `sql` holds a `%s` placeholder.
///
/// `%I` becomes the unknown name `"%"`. `%L` becomes a string literal that
/// holds `LITERAL_PLACEHOLDER`, so code built from it reads as unreadable.
/// `%%` is a literal `%`. A placeholder may carry a position, flags and a
/// width, as in `%1$-10I`.
fn fill_placeholders(sql: &str) -> (String, bool) {
    let chars: Vec<char> = sql.chars().collect();
    let mut out = String::new();
    let mut text_placeholder = false;
    let mut i = 0;
    while let Some(&c) = chars.get(i) {
        if c != '%' {
            out.push(c);
            i += 1;
            continue;
        }
        if chars.get(i + 1) == Some(&'%') {
            out.push('%');
            i += 2;
            continue;
        }
        let mut j = i + 1;
        while chars
            .get(j)
            .is_some_and(|c| c.is_ascii_digit() || matches!(c, '$' | '-' | '*'))
        {
            j += 1;
        }
        // `%s` inserts any text, so `EXECUTE` marks it as SQL it cannot read.
        if chars.get(j) == Some(&'I') {
            out.push_str("\"%\"");
            i = j + 1;
        } else if chars.get(j) == Some(&'L') {
            out.extend(['\'', LITERAL_PLACEHOLDER, '\'']);
            i = j + 1;
        } else if chars.get(j) == Some(&'s') {
            out.push_str("\"%\"");
            text_placeholder = true;
            i = j + 1;
        } else {
            out.push(c);
            i += 1;
        }
    }
    (out, text_placeholder)
}

/// The length of a dollar-quote delimiter such as `$$` or `$body$`.
///
/// Returns `None` for anything else, such as a `$1` parameter.
fn dollar_tag_len(rest: &[char]) -> Option<usize> {
    if rest.first() != Some(&'$') {
        return None;
    }
    let tag = rest[1..].iter().take_while(|c| is_ident(**c)).count();
    if tag > 0 && rest[1].is_ascii_digit() {
        return None;
    }
    (rest.get(1 + tag) == Some(&'$')).then_some(tag + 2)
}

// ── Statement matching ───────────────────────────────────────────────────────

/// What a matched statement does to its table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    /// An index build, drop or rebuild.
    Index { concurrent: bool },
    /// Any other statement that takes a blocking lock.
    Lock,
}

/// One statement that can lock a table.
#[derive(Debug)]
struct Hit {
    /// The token index where the statement starts.
    at: usize,
    line: usize,
    verb: &'static str,
    /// The index the statement names, if any.
    index: Option<String>,
    /// The table the statement locks. `None` when the lint cannot tell.
    table: Option<String>,
    kind: Kind,
    /// Whether the statement sits inside a dollar-quoted body.
    in_body: bool,
    /// The first token of the code that does not run now around the
    /// statement, such as a function body. `None` when the statement runs.
    body_start: Option<usize>,
    /// Whether the table is hot here. An unknown table counts as hot.
    hot: bool,
}

impl Hit {
    /// A short label for a failure message.
    fn label(&self) -> String {
        self.index.as_ref().map_or_else(
            || self.verb.to_string(),
            |index| format!("{} {index}", self.verb),
        )
    }

    fn table_name(&self) -> &str {
        self.table.as_deref().unwrap_or("an unknown table")
    }
}

/// The tokens, comments and lock-taking statements of one `up.sql`.
struct Analysis {
    comments: Vec<Comment>,
    hits: Vec<Hit>,
    /// Each `lock_timeout` change: its token index, and whether it sets a bound.
    timeouts: Vec<(usize, Timeout)>,
    /// Each change in code that does not run now, such as a function body.
    body_timeouts: Vec<(usize, Timeout)>,
    /// The number of top-level statements.
    statement_count: usize,
    /// The first token and the end of each routine body, from `routine_bodies`.
    bodies: Vec<(usize, usize)>,
}

/// A view of the tokens with statement boundaries.
struct Stmts<'a> {
    toks: &'a [Token],
    /// For each token, the index of the first token of its statement.
    starts: Vec<usize>,
}

impl<'a> Stmts<'a> {
    fn new(toks: &'a [Token]) -> Self {
        let mut starts = Vec::with_capacity(toks.len());
        // Per dollar-quote depth: the start of the open statement, if any, and
        // how many SQL `CASE` expressions are open in it. A `THEN` or `ELSE`
        // of such an expression is not PL/pgSQL control flow.
        let mut open: Vec<(Option<usize>, usize)> = Vec::new();
        for k in 0..toks.len() {
            let depth = toks[k].depth;
            let entering = k == 0 || depth > toks[k - 1].depth;
            if entering {
                open.truncate(depth);
            }
            open.resize(depth + 1, (None, 0));
            // A body starts a statement. After the body, the statement around
            // it goes on, so a literal never splits it.
            let (start, cases) = &mut open[depth];
            // A quoted word is a name, never a keyword.
            // `BEGIN ATOMIC` opens an inline SQL body at the depth of its
            // `CREATE`, so its first statement starts after `ATOMIC`.
            let word = |j: usize| !toks[j].quoted && toks[j].depth == depth;
            let atomic = k >= 2
                && word(k - 1)
                && word(k - 2)
                && toks[k - 1].tok == Tok::Word("atomic".to_string())
                && toks[k - 2].tok == Tok::Word("begin".to_string());
            let boundary = entering
                || start.is_none()
                || atomic
                || (depth > 0
                    && k > 0
                    && toks[k - 1].depth == depth
                    && !toks[k - 1].quoted
                    && match &toks[k - 1].tok {
                        Tok::Word(w) if ["begin", "loop"].contains(&w.as_str()) => true,
                        Tok::Word(w) if ["then", "else"].contains(&w.as_str()) => *cases == 0,
                        _ => false,
                    });
            if boundary {
                *start = Some(k);
                *cases = 0;
            }
            // A `CASE` that does not start a statement is an expression.
            match &toks[k].tok {
                _ if toks[k].quoted => {}
                Tok::Word(w) if w == "case" && !boundary => *cases += 1,
                Tok::Word(w) if w == "end" && *cases > 0 => *cases -= 1,
                _ => {}
            }
            starts.push(start.unwrap_or(k));
            if toks[k].tok == Tok::Punct(';') {
                *start = None;
            }
        }
        Self { toks, starts }
    }

    fn word(&self, k: usize) -> Option<&'a str> {
        match &self.toks.get(k)?.tok {
            Tok::Word(w) => Some(w),
            _ => None,
        }
    }

    fn is(&self, k: usize, expected: &str) -> bool {
        self.word(k) == Some(expected)
    }

    /// Whether the token at `k` is the unquoted keyword `expected`.
    fn keyword(&self, k: usize, expected: &str) -> bool {
        self.is(k, expected) && !self.toks[k].quoted
    }

    fn is_punct(&self, k: usize, expected: char) -> bool {
        self.toks
            .get(k)
            .is_some_and(|t| t.tok == Tok::Punct(expected))
    }

    fn string(&self, k: usize) -> Option<&'a str> {
        match &self.toks.get(k)?.tok {
            Tok::Str(s) => Some(s),
            _ => None,
        }
    }

    /// Whether the call at `k` is a whole statement: `SELECT f(...)` or
    /// `PERFORM f(...)`, with nothing after the closing parenthesis.
    fn is_bare_call(&self, k: usize) -> bool {
        let start = self.starts[k];
        // Only `pg_catalog` surely holds the built-in function.
        let named = k == start + 1
            || (k == start + 3
                && self.is(start + 1, "pg_catalog")
                && self.is_punct(start + 2, '.'));
        if !named || !(self.keyword(start, "select") || self.keyword(start, "perform")) {
            return false;
        }
        let mut parens = 0_usize;
        for j in k + 1..self.toks.len() {
            if self.is_punct(j, '(') {
                parens += 1;
            } else if self.is_punct(j, ')') {
                parens -= 1;
                if parens == 0 {
                    return j + 1 == self.end(k);
                }
            }
        }
        false
    }

    /// The index one past the last token of the statement that holds `k`.
    ///
    /// The range takes in any body nested in the statement.
    fn end(&self, k: usize) -> usize {
        let depth = self.toks[self.starts[k]].depth;
        (k..self.toks.len())
            .find(|&j| {
                let d = self.toks[j].depth;
                d < depth
                    || (d == depth && (self.starts[j] != self.starts[k] || self.is_punct(j, ';')))
            })
            .unwrap_or(self.toks.len())
    }

    /// Read a name that may carry a schema. Return the name as written and the next index.
    ///
    /// A name never runs past the end of the body that holds it.
    fn qualified_name(&self, k: usize) -> Option<(String, usize)> {
        let mut name = self.word(k)?.to_string();
        let depth = self.toks[k].depth;
        let mut k = k + 1;
        while self.is_punct(k, '.') && self.toks.get(k + 1).is_some_and(|t| t.depth == depth) {
            let Some(part) = self.word(k + 1) else {
                break;
            };
            name.push('.');
            name.push_str(part);
            k += 2;
        }
        Some((name, k))
    }

    /// Read a comma-separated list of names. Each name may carry `ONLY`.
    fn name_list(&self, mut k: usize) -> Vec<String> {
        let mut names = Vec::new();
        loop {
            if self.keyword(k, "only") {
                k += 1;
            }
            let Some((name, next)) = self.qualified_name(k) else {
                break;
            };
            names.push(name);
            // `name *` asks for the descendant tables too, which is the default.
            let next = next + usize::from(self.is_punct(next, '*'));
            let same_body = self
                .toks
                .get(next)
                .is_some_and(|t| t.depth == self.toks[k].depth);
            if !same_body || !self.is_punct(next, ',') {
                break;
            }
            k = next + 1;
        }
        names
    }

    /// Skip `IF EXISTS` or `IF NOT EXISTS` at `k`.
    fn skip_if_exists(&self, k: usize) -> usize {
        if !self.keyword(k, "if") {
            return k;
        }
        let j = if self.keyword(k + 1, "not") {
            k + 2
        } else {
            k + 1
        };
        if self.keyword(j, "exists") { j + 1 } else { k }
    }

    /// The name after the first unquoted `keyword` in the statement that
    /// holds `k`.
    fn name_after(&self, k: usize, keyword: &str) -> Option<String> {
        (k..self.end(k))
            .find(|&j| self.keyword(j, keyword))
            .and_then(|j| self.qualified_name(j + 1))
            .map(|(name, _)| name)
    }

    /// Whether the statement that holds `k` has the unquoted keyword `first`
    /// directly before `second`.
    fn has_pair(&self, k: usize, first: &str, second: &str) -> bool {
        (self.starts[k]..self.end(k)).any(|j| self.keyword(j, first) && self.keyword(j + 1, second))
    }

    /// The comma-separated actions of the statement that starts at `k`, as
    /// token ranges. A comma inside parentheses does not split an action.
    fn actions(&self, k: usize) -> Vec<(usize, usize)> {
        let end = self.end(k);
        let mut out = Vec::new();
        let mut from = k;
        let mut parens = 0_usize;
        for j in k..end {
            if self.is_punct(j, '(') {
                parens += 1;
            } else if self.is_punct(j, ')') {
                parens = parens.saturating_sub(1);
            } else if parens == 0 && self.is_punct(j, ',') {
                out.push((from, j));
                from = j + 1;
            }
        }
        out.push((from, end));
        out
    }

    /// The table a `CREATE TABLE` or `ALTER TABLE` statement at `start` names.
    fn statement_table(&self, start: usize) -> Option<String> {
        let mut j = start + 1;
        if self.keyword(start, "create") {
            if self.keyword(j, "global") || self.keyword(j, "local") {
                j += 1;
            }
            if ["temp", "temporary", "unlogged"]
                .iter()
                .any(|w| self.keyword(j, w))
            {
                j += 1;
            }
        }
        if !(self.keyword(start, "create") || self.keyword(start, "alter"))
            || !self.keyword(j, "table")
        {
            return None;
        }
        let mut j = self.skip_if_exists(j + 1);
        if self.keyword(j, "only") {
            j += 1;
        }
        self.qualified_name(j).map(|(name, _)| name)
    }
}

/// The new history key of `index` after the `ALTER INDEX` at `at`, if the
/// statement renames it or moves it to another schema.
fn moved_index_key(s: &Stmts, at: usize, index: &str) -> Option<String> {
    let end = s.end(at);
    if let Some(j) = (at..end).find(|&j| s.keyword(j, "rename") && s.keyword(j + 1, "to")) {
        return Some(index_key(index, s.word(j + 2)?));
    }
    let j = (at..end).find(|&j| s.keyword(j, "set") && s.keyword(j + 1, "schema"))?;
    Some(format!("{}.{}", s.word(j + 2)?, base(index)))
}

/// The history key of `index`, in the schema that `owner` names.
///
/// Postgres puts an index in the schema of its table. A name without a schema
/// goes to the first schema of `search_path`. The connection may set another
/// path, so that schema is unknown. Such a key matches only a name without a
/// schema, which resolves on the same path.
fn index_key(owner: &str, index: &str) -> String {
    let schema = owner
        .rsplit_once('.')
        .map_or(PATH_SCHEMA, |(schema, _)| schema);
    format!("{schema}.{}", base(index))
}

/// Stands in for the unknown first schema of `search_path` in a history key.
/// Parentheses never occur in an unquoted name.
const PATH_SCHEMA: &str = "(search_path)";

/// Stands in for a `.` inside a quoted identifier.
///
/// `"a.b"` is one name, so it must never read as schema `a` and table `b`.
/// This character never occurs in an unquoted name.
const QUOTED_DOT: char = '\u{2024}';

/// The value of a `format()` `%L` placeholder.
///
/// A `DO` or `EXECUTE` may run that literal as code. This character never
/// occurs in a migration, so the lexer finds it only where `%L` sits.
const LITERAL_PLACEHOLDER: char = '\u{E000}';

/// Whether `c` can be part of an unquoted identifier or a dollar tag.
///
/// Postgres accepts every non-ASCII character there, as well as letters,
/// digits and `_`.
fn is_ident(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || !c.is_ascii()
}

/// The last part of a name that may carry a schema.
///
/// The hot-table list and the history match on this part, so a schema never
/// hides a hot table. A new-table exemption matches the whole name instead.
fn base(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}

/// A statement before the history and positional rules apply.
struct Raw {
    at: usize,
    verb: &'static str,
    index: Option<String>,
    table: Option<String>,
    kind: Kind,
}

impl Raw {
    const fn lock(at: usize, verb: &'static str, table: Option<String>) -> Self {
        Self {
            at,
            verb,
            index: None,
            table,
            kind: Kind::Lock,
        }
    }
}

/// Analyse one `up.sql`, and add what it creates to `history`.
fn analyse(sql: &str, history: &mut History) -> Analysis {
    let (mut toks, comments) = tokenize(sql);
    blank_foreign_bodies(&mut toks);
    let s = Stmts::new(&toks);
    let mut raws: Vec<Raw> = Vec::new();
    let mut timeouts = Vec::new();
    let mut body_timeouts = Vec::new();
    // A table counts as new from its `CREATE TABLE` on. `IF NOT EXISTS` can
    // do nothing, so it does not count.
    let mut created: BTreeMap<String, usize> = BTreeMap::new();
    // A function body, or a branch that may not run, cannot make a table new.
    // Its locks still count, which fails closed.
    let mut not_run: BTreeMap<String, usize> = BTreeMap::new();
    let unconditional = unconditional(&s);
    let opaque = opaque_points(&s, history);
    let path_change = path_change(&s, &opaque, history);
    let path_bodies = path_bodies(&s);

    for (k, tok) in toks.iter().enumerate() {
        let start = s.starts[k] == k;
        // A quoted word is a name, never a statement keyword.
        match s.word(k).filter(|_| !tok.quoted) {
            Some("create") if start => {
                let sure = tok.runs && unconditional[k];
                let created = if sure { &mut created } else { &mut not_run };
                create(&s, k, &mut raws, created);
                // A partitioned table is learnt even from a branch, which
                // fails closed.
                if s.has_pair(k, "partition", "by")
                    && let Some(table) = s.statement_table(k)
                {
                    history.partitioned.insert(base(&table).to_string());
                }
            }
            Some("drop") if start => drop(&s, k, history, &mut raws),
            Some("alter") if start => alter(&s, k, history, &mut raws),
            Some("rename") if s.keyword(k + 1, "to") => rename(&s, k, history),
            Some("lock" | "truncate") if start => raws.extend(lock_or_truncate(&s, k)),
            Some("cluster") if start => raws.push(cluster(&s, k)),
            Some("vacuum") if start => raws.extend(vacuum_full(&s, k)),
            // SQL the lint cannot read may lock anything, and may also clear
            // the bound for what comes after it.
            Some("execute") if dynamic_execute(&s, k) => {
                if let Some(raw) = unreadable_execute(&s, k) {
                    raws.push(raw);
                    let list = if tok.runs {
                        &mut timeouts
                    } else {
                        &mut body_timeouts
                    };
                    list.push((
                        k,
                        Timeout::Set {
                            bounds: false,
                            local: false,
                        },
                    ));
                }
            }
            Some("reindex") if start => raws.extend(reindex(&s, k)),
            Some("references") => raws.extend(references(&s, k, history)),
            Some("partition") if s.keyword(k + 1, "of") => {
                let parent = s.qualified_name(k + 2).map(|(t, _)| t);
                let child = s.statement_table(s.starts[k]);
                learn_partition(history, parent.as_deref(), child.as_deref());
                raws.push(Raw::lock(s.starts[k], "PARTITION OF", parent));
            }
            Some("attach") if s.keyword(k + 1, "partition") => {
                let parent = s.statement_table(s.starts[k]);
                let child = s.qualified_name(k + 2).map(|(t, _)| t);
                learn_partition(history, parent.as_deref(), child.as_deref());
            }
            // A function body keeps its own changes, for the locks in it.
            _ => {
                let change = recorded_change(&s, k, unconditional[k], (path_change, &path_bodies));
                let list = if tok.runs {
                    &mut timeouts
                } else {
                    &mut body_timeouts
                };
                list.extend(change.map(|change| (execute_at(&s, k), change)));
            }
        }
    }

    function_settings(&s, &mut body_timeouts);
    foreign_do_bodies(&s, &mut raws, &mut timeouts, &mut body_timeouts);
    let settings = (path_change, opaque.as_slice());
    unreadable_settings(&s, sql, &unconditional, settings, history, &mut raws);
    call_clears(
        &s,
        history,
        path_change,
        &mut raws,
        &mut timeouts,
        &mut body_timeouts,
    );
    let new_tables = new_table_spans(&s, &created, history);
    let hits = resolve(raws, &s, &unconditional, &new_tables, path_change, history);

    let statement_count = (0..toks.len())
        .filter(|&k| s.starts[k] == k && toks[k].depth == 0 && !s.is_punct(k, ';'))
        .count();
    let analysis = Analysis {
        comments,
        hits,
        timeouts,
        body_timeouts,
        statement_count,
        bodies: routine_bodies(&s),
    };
    record_self_bounded(&s, &analysis, history);
    analysis
}

/// Add each `CREATE FUNCTION ... SET lock_timeout` clause to `body_timeouts`.
///
/// Postgres applies the clause on each call, before the body runs. So the
/// clause counts as a session value at the first token of the body.
fn function_settings(s: &Stmts, body_timeouts: &mut Vec<(usize, Timeout)>) {
    let mut settings = Vec::new();
    for k in 0..s.toks.len() {
        // An atomic or `RETURN` body sits at the depth of its `CREATE`.
        let Some((body, end)) = routine_body(s, k) else {
            continue;
        };
        for set in routine_clauses(s, k, end, "lock_timeout") {
            let value = if s.is_punct(set + 2, '=') || s.keyword(set + 2, "to") {
                set + 3
            } else {
                set + 2
            };
            let bounds = bounds_wait(s, value);
            settings.push((
                body,
                Timeout::Set {
                    bounds,
                    local: false,
                },
            ));
        }
    }
    // `timeout_in_force` reads the changes in token order. Postgres applies
    // a clause before the body runs, so the stable sort keeps each clause
    // ahead of a body change at the same token.
    settings.append(body_timeouts);
    settings.sort_by_key(|(k, _)| *k);
    *body_timeouts = settings;
}

/// Each `SET <name>` clause of the routine that `CREATE` starts at `k`.
///
/// A clause sits outside the parentheses of the signature, and starts with an
/// unquoted `SET`.
fn routine_clauses(s: &Stmts, k: usize, end: usize, name: &str) -> Vec<usize> {
    let depth = s.toks[k].depth;
    let mut parens = 0_usize;
    let mut sets = Vec::new();
    for t in (k..end).filter(|&t| s.toks[t].depth == depth) {
        if s.is_punct(t, '(') {
            parens += 1;
        } else if s.is_punct(t, ')') {
            parens = parens.saturating_sub(1);
        } else if parens == 0 && s.keyword(t, "set") && s.is(t + 1, name) {
            sets.push(t);
        }
    }
    sets
}

/// The body of each routine with a `SET search_path` clause.
///
/// The body runs with that path, so an unqualified `set_config` in it may be
/// a user function that shadows the built-in.
fn path_bodies(s: &Stmts) -> Vec<(usize, usize)> {
    (0..s.toks.len())
        .filter_map(|k| routine_body(s, k).map(|body| (k, body)))
        .filter(|&(k, (_, end))| !routine_clauses(s, k, end, "search_path").is_empty())
        .map(|(_, body)| body)
        .collect()
}

/// Treat each `DO` body in another language as unreadable code.
///
/// The lint reads only PL/pgSQL. A body in another language, such as
/// `plpython3u`, may lock any table. It may also clear the bound for what
/// comes after it, so a session clear follows the statement.
fn foreign_do_bodies(
    s: &Stmts,
    raws: &mut Vec<Raw>,
    timeouts: &mut Vec<(usize, Timeout)>,
    body_timeouts: &mut Vec<(usize, Timeout)>,
) {
    for k in (0..s.toks.len()).filter(|&k| foreign_do(s, k)) {
        let end = s.end(k);
        raws.push(Raw::lock(k, FOREIGN_CODE, None));
        let list = if s.toks[k].runs {
            &mut *timeouts
        } else {
            &mut *body_timeouts
        };
        list.push((
            end,
            Timeout::Set {
                bounds: false,
                local: false,
            },
        ));
    }
}

/// Whether a `DO` in a language other than PL/pgSQL starts at `k`.
fn foreign_do(s: &Stmts, k: usize) -> bool {
    s.starts[k] == k && s.keyword(k, "do") && language(s, k).is_some_and(|l| l != "plpgsql")
}

/// The `LANGUAGE` clause of the statement that starts at `k`, if any.
///
/// The clause is a `LANGUAGE` keyword outside any parentheses, followed by a
/// name. So a routine or a parameter named `language` is not the clause.
/// The lexer folds an unquoted name to lower case. A quoted name keeps its
/// case, so `"PLPGSQL"` names another language.
fn language<'a>(s: &Stmts<'a>, k: usize) -> Option<&'a str> {
    let depth = s.toks[k].depth;
    let mut parens = 0_usize;
    for j in (k..s.end(k)).filter(|&j| s.toks[j].depth == depth) {
        if s.is_punct(j, '(') {
            parens += 1;
        } else if s.is_punct(j, ')') {
            parens = parens.saturating_sub(1);
        } else if parens == 0
            && s.keyword(j, "language")
            && let Some(name) = s.word(j + 1).or_else(|| s.string(j + 1))
        {
            return Some(name);
        }
    }
    None
}

/// The verb of a lock that any string literal may hide once
/// `standard_conforming_strings` is off.
const NONSTANDARD_STRINGS: &str = "SQL after standard_conforming_strings is off";

/// Add the locks of settings that may hide or expose a lock.
fn unreadable_settings(
    s: &Stmts,
    sql: &str,
    unconditional: &[bool],
    (path_change, opaque): (Option<usize>, &[usize]),
    history: &mut History,
    raws: &mut Vec<Raw>,
) {
    nonstandard_strings(s, sql, unconditional, (path_change, opaque), history, raws);
    routine_resets(s, history, raws);
}

/// Treat each statement that may turn `standard_conforming_strings` off as
/// unreadable code.
///
/// With the setting off, a plain `'...'` literal takes backslash escapes, as
/// `E'...'` does. The lint then misreads every later `DO`, routine and
/// `EXECUTE` body, so the statement counts as a lock that no bound covers.
///
/// While the setting is off, each top-level statement with a backslash on its
/// lines counts as unreadable too. The lock sits on that statement, so an
/// annotation on the setter cannot cover it. A session value outlives its
/// file, so the state carries into later files. A top-level local value ends
/// at the commit. Without a backslash, the setting changes nothing.
fn nonstandard_strings(
    s: &Stmts,
    sql: &str,
    unconditional: &[bool],
    (path_change, opaque): (Option<usize>, &[usize]),
    history: &mut History,
    raws: &mut Vec<Raw>,
) {
    let backslash_lines: BTreeSet<usize> = sql
        .lines()
        .enumerate()
        .filter(|(_, text)| text.contains('\\'))
        .map(|(n, _)| n + 1)
        .collect();
    // The session value, and a transaction-local `off` above it. The session
    // value when the transaction began, and whether the transaction has turned
    // either value off. A rollback restores the saved value. A rollback to a
    // savepoint may restore any value from the transaction.
    let mut session = history.nonstandard_strings;
    let mut local = false;
    let mut hidden = false;
    let mut saved = session;
    let (mut any_session_off, mut any_local_off) = (session, false);
    for (k, &surely_runs) in unconditional.iter().enumerate() {
        // An `off` counts wherever it sits, which fails closed. An `on`, a
        // commit or a rollback counts only where it surely runs.
        let sure = s.toks[k].runs && surely_runs;
        if sure && s.starts[k] == k {
            let end = s.keyword(k, "end") && s.toks[k].depth == 0;
            if s.keyword(k, "commit") || end {
                local = false;
                saved = session;
                (any_session_off, any_local_off) = (session, false);
            } else if s.keyword(k, "rollback") || s.keyword(k, "abort") {
                if (k + 1..=k + 2).any(|j| s.keyword(j, "to")) {
                    session |= any_session_off;
                    local |= any_local_off;
                } else {
                    session = saved;
                    local = false;
                }
            }
        }
        let top_start = s.starts[k] == k && s.toks[k].depth == 0 && !s.is_punct(k, ';');
        if top_start && (session || local || hidden) {
            let last = s.end(k).saturating_sub(1).max(k);
            let lines = s.toks[k].line..=s.toks[last].line;
            if backslash_lines.range(lines).next().is_some() {
                raws.push(Raw::lock(k, NONSTANDARD_STRINGS, None));
            }
        }
        // Code that the lint cannot read may turn the setting off. Its own
        // finding covers that code. The doubt holds for the rest of the file,
        // but history keeps only the values the lint can read.
        hidden |= opaque.contains(&k);
        // A routine body may run in a later transaction, so only a top-level
        // local value ends at the commit.
        let top = s.toks[k].depth == 0;
        match conforming_change(s, k, path_change).map(|(on, local)| (on, local && top)) {
            Some((false, true)) => {
                raws.push(Raw::lock(s.starts[k], NONSTANDARD_STRINGS, None));
                local = true;
                any_local_off = true;
            }
            Some((false, false)) => {
                raws.push(Raw::lock(s.starts[k], NONSTANDARD_STRINGS, None));
                (session, local) = (true, false);
                any_session_off = true;
            }
            Some((true, _)) if sure => (session, local) = (false, false),
            Some((true, _)) | None => {}
        }
    }
    // The file ends its transaction, so only the session value carries.
    history.nonstandard_strings = session;
}

/// The new `standard_conforming_strings` value that the token at `k` sets, if
/// any, and whether that value is local to the transaction. A value the lint
/// cannot read counts as `false`, which fails closed.
///
/// Only a session value counts as `true`. A transaction-local `on` ends at
/// the commit and restores the session value, so it changes nothing. A scope
/// the lint cannot read counts as the session.
fn conforming_change(s: &Stmts, k: usize, path_change: Option<usize>) -> Option<(bool, bool)> {
    let literal = |j: usize| s.word(j).or_else(|| s.string(j));
    let on = |j: usize| literal(j).is_some_and(pg_true);
    let start = s.starts[k] == k;
    if start && s.keyword(k, "reset") {
        let resets = s.is(k + 1, "standard_conforming_strings") || s.keyword(k + 1, "all");
        return resets.then_some((true, false));
    }
    if start && s.keyword(k, "set") {
        let name = if s.keyword(k + 1, "local") || s.keyword(k + 1, "session") {
            k + 2
        } else {
            k + 1
        };
        let value = if s.is_punct(name + 1, '=') || s.keyword(name + 1, "to") {
            name + 2
        } else {
            name + 1
        };
        let local = s.keyword(k + 1, "local");
        return s
            .is(name, "standard_conforming_strings")
            .then(|| (on(value), local))
            .filter(|&(on, local)| !(on && local));
    }
    let call = s.is(k, "set_config") && s.is_punct(k + 1, '(');
    // Another schema's `set_config`, or an unqualified one after a path
    // change, may be a user function. So it never turns the setting on. A
    // name that the lint cannot read counts as a session `off`.
    let schema = (k >= 2 && s.is_punct(k - 1, '.')).then(|| s.word(k - 2));
    // An unqualified call resolves before its own statement changes the path.
    let built_in = schema.map_or_else(
        || path_change.is_none_or(|c| c >= s.starts[k]),
        |name| name == Some("pg_catalog"),
    );
    if call && !built_in {
        return None;
    }
    let name = call.then(|| set_config_name(s, k));
    if name == Some(None) {
        return Some((false, false));
    }
    let named = name
        .flatten()
        .is_some_and(|n| n.eq_ignore_ascii_case("standard_conforming_strings"));
    let scope = |read: fn(&str) -> bool| s.is_punct(k + 5, ',') && literal(k + 6).is_some_and(read);
    let (session, local) = (scope(pg_false), scope(pg_true));
    named
        .then(|| (s.is_punct(k + 3, ',') && on(k + 4), local))
        .filter(|&(on, _)| !on || session)
}

/// The setting that the `set_config` call at `k` names, if it is one literal.
///
/// The name may carry a cast, parentheses or the `setting_name =>` label. In
/// an `EXECUTE` statement, the lexer reads a literal as SQL, so its text is a
/// deeper token. Any other name, such as `'a' || 'b'`, is unknown.
fn set_config_name(s: &Stmts, k: usize) -> Option<String> {
    let depth = s.toks[k].depth;
    let mut parens = 0_usize;
    let mut name = None;
    for j in k + 1..s.toks.len() {
        let tok = &s.toks[j];
        let text = match &tok.tok {
            Tok::Str(v) => Some(v.as_str()),
            Tok::Word(w) if tok.depth > depth => Some(w.as_str()),
            _ => None,
        };
        if tok.depth > depth && text.is_none() {
            return None;
        }
        match &tok.tok {
            _ if text.is_some() => {
                if name.is_some() {
                    return None;
                }
                name = text.map(|t| t.trim().to_string());
            }
            Tok::Punct('(') => parens += 1,
            Tok::Punct(')') => parens = parens.checked_sub(1)?,
            Tok::Punct(',') if parens == 1 => return name,
            Tok::Punct(':' | '=' | '>') => {}
            Tok::Word(w) if ["text", "varchar", "name", "setting_name"].contains(&w.as_str()) => {}
            _ => return None,
        }
    }
    None
}

/// The verb of a lock that an `ALTER` of a routine may expose.
const ROUTINE_RESET: &str = "ALTER of a routine that drops its lock_timeout setting";

/// Treat each `ALTER FUNCTION`, `PROCEDURE` or `ROUTINE` that removes or
/// clears a `lock_timeout` setting as a lock that no bound covers.
///
/// The routine body may lock a hot table, and its `CREATE` may have set the
/// bound. After the `ALTER`, each call runs that lock without the bound.
fn routine_resets(s: &Stmts, history: &mut History, raws: &mut Vec<Raw>) {
    for k in (0..s.toks.len()).filter(|&k| s.starts[k] == k && s.keyword(k, "alter")) {
        if !["function", "procedure", "routine"]
            .iter()
            .any(|w| s.keyword(k + 1, w))
        {
            continue;
        }
        let depth = s.toks[k].depth;
        let clause = |j: usize| s.toks[j].depth == depth;
        let reset = (k..s.end(k)).filter(|&j| clause(j)).any(|j| {
            s.keyword(j, "reset") && (s.is(j + 1, "lock_timeout") || s.keyword(j + 1, "all"))
        });
        let clears = (k..s.end(k)).filter(|&j| clause(j)).any(|j| {
            s.keyword(j, "set") && s.is(j + 1, "lock_timeout") && {
                let value = if s.is_punct(j + 2, '=') || s.keyword(j + 2, "to") {
                    j + 3
                } else {
                    j + 2
                };
                !bounds_wait(s, value)
            }
        });
        if reset || clears {
            raws.push(Raw::lock(k, ROUTINE_RESET, None));
        }
        alter_routine_history(s, k, clears, history);
    }
}

/// Update the routine history for the `ALTER` of a routine at `k`.
///
/// A setting that clears the bound makes the routine a clearing routine. A
/// rename carries what the history knows to the new name. A rename or a
/// schema move drops each exact identity of the old name, because another
/// routine may now hold it.
fn alter_routine_history(s: &Stmts, k: usize, clears: bool, history: &mut History) {
    let Some((name, _)) = s.qualified_name(k + 2) else {
        return;
    };
    let old = base(&name).to_string();
    if clears {
        history.clearing_routines.insert(old.clone());
    }
    let renamed = (k..s.end(k))
        .find(|&j| s.keyword(j, "rename") && s.keyword(j + 1, "to"))
        .and_then(|j| s.word(j + 2));
    // The `ALTER` may drop the routine's own bound, or give its name to
    // another routine.
    history.unbounded_routines.insert(old.clone());
    if let Some(new) = renamed {
        history.unbounded_routines.insert(new.to_string());
        for set in [
            &mut history.clearing_routines,
            &mut history.foreign_routines,
            &mut history.locking_routines,
            &mut history.path_routines,
        ] {
            if set.contains(&old) {
                set.insert(new.to_string());
            }
        }
    }
    if clears || renamed.is_some() || s.has_pair(k, "set", "schema") {
        history
            .bounded_routines
            .retain(|id| base(id.split('/').next().unwrap_or(id)) != old);
    }
}

/// The verb of a lock that code in another language may take.
const FOREIGN_CODE: &str = "code in another language";

/// The index of `FUNCTION` or `PROCEDURE` when a routine `CREATE` starts at `k`.
fn routine_keyword(s: &Stmts, k: usize) -> Option<usize> {
    if s.starts[k] != k || !s.keyword(k, "create") {
        return None;
    }
    let j = if s.keyword(k + 1, "or") && s.keyword(k + 2, "replace") {
        k + 3
    } else {
        k + 1
    };
    (s.keyword(j, "function") || s.keyword(j, "procedure")).then_some(j)
}

/// One routine call, or one routine that this file creates.
struct Routine {
    /// The name as written, with any schema.
    name: String,
    /// The number of arguments or parameters. `None` when the lint cannot count them.
    arity: Option<usize>,
    /// The fewest arguments a call may pass. A parameter with a default may
    /// be left out. For a call, this is its own argument count.
    min_arity: Option<usize>,
    /// The first token: the `CALL`, the called name, or the `CREATE`.
    at: usize,
    /// Whether the body is in a language other than PL/pgSQL or SQL.
    foreign: bool,
    /// Whether the `CREATE` surely runs and stays. A routine created in an
    /// uncalled body or a branch may not exist. A later `ROLLBACK` may undo
    /// the `CREATE`. A call is always sure.
    sure: bool,
}

impl Routine {
    /// Whether `call` passes a number of arguments this routine accepts.
    const fn accepts(&self, call: &Self) -> bool {
        match (self.min_arity, self.arity, call.arity) {
            (Some(fewest), Some(most), Some(n)) => fewest <= n && n <= most,
            _ => false,
        }
    }
}

/// Add a session clear at each call that may clear the bound.
///
/// A clear in a routine body can outlive the call. For example, a
/// `set_config(..., true)` holds until the transaction ends. A call clears
/// when the body in this file changes `lock_timeout` other than to set a
/// bound, or calls such a routine.
///
/// A call reaches a body in this file only when an earlier `CREATE` has the
/// same name as written and the same number of parameters. Any other `CALL`
/// clears, because it may reach a routine from another file. A call also
/// clears when any such `CREATE` clears, or when an earlier migration created
/// a clearing routine of that name. After a `search_path` change, an
/// unqualified call matches no `CREATE`.
///
/// A routine in another language may lock any table and clear the bound. A
/// call of such a routine counts as a lock on an unknown table.
fn call_clears(
    s: &Stmts,
    history: &mut History,
    path_change: Option<usize>,
    raws: &mut Vec<Raw>,
    timeouts: &mut Vec<(usize, Timeout)>,
    body_timeouts: &mut Vec<(usize, Timeout)>,
) {
    let routines = file_routines(s);
    let inherited = history.clearing_routines.clone();
    let inherited_foreign = history.foreign_routines.clone();
    let inherited_locking = history.locking_routines.clone();
    let inherited_bounded = history.bounded_routines.clone();
    let bases: BTreeSet<&str> = routines
        .iter()
        .map(|r| base(&r.name))
        .chain(inherited.iter().map(String::as_str))
        .chain(inherited_locking.iter().map(String::as_str))
        .collect();
    let calls: Vec<Routine> = (0..s.toks.len())
        .filter_map(|k| call_target(s, k, &bases))
        .collect();
    // Whether every earlier `CREATE` that may match the call keeps the bound.
    // The lint does not compare parameter types, so any overload with the
    // same name and arity may be the one that runs.
    let path_bodies = path_bodies(s);
    let keeps = |call: &Routine, clearing: &BTreeSet<usize>| {
        let mut matches = (0..routines.len()).filter(|&i| reaches(s, &routines[i], call));
        let first = matches.next();
        !unplaced_call(call, path_change, &path_bodies)
            && !inherited.contains(base(&call.name))
            && first.is_some()
            && first
                .into_iter()
                .chain(matches)
                .all(|i| !clearing.contains(&i))
    };
    // A change or a call in a nested routine body belongs to that routine.
    let bodies = routine_bodies(s);
    let changes = |r: &Routine| {
        body_timeouts.iter().any(|(k, change)| {
            owns(s, &bodies, r.at, *k) && !matches!(change, Timeout::Set { bounds: true, .. })
        })
    };
    // A routine that calls a clearing routine clears too.
    let mut clearing: BTreeSet<usize> = (0..routines.len())
        .filter(|&i| routines[i].foreign || changes(&routines[i]))
        .collect();
    loop {
        let before = clearing.len();
        for (i, r) in routines.iter().enumerate() {
            let calls_a_clearer = calls
                .iter()
                .any(|call| owns(s, &bodies, r.at, call.at) && !keeps(call, &clearing));
            if calls_a_clearer {
                clearing.insert(i);
            }
        }
        if clearing.len() == before {
            break;
        }
    }
    let clear = Timeout::Set {
        bounds: false,
        local: false,
    };
    for call in &calls {
        let callee = base(&call.name);
        let foreign = inherited_foreign.contains(callee)
            || routines
                .iter()
                .any(|r| r.foreign && r.at < call.at && base(&r.name) == callee);
        if foreign {
            raws.push(Raw::lock(call.at, FOREIGN_CODE, None));
        }
        // A `CALL` that no earlier `CREATE` here matches, or a call of a
        // locking routine from an earlier migration, may take any lock.
        let unplaced = unplaced_call(call, path_change, &path_bodies);
        // The lint does not compare types. So an earlier locking overload of
        // the same name and arity may be the one that runs.
        let resolved = !unplaced
            && !inherited_locking.contains(callee)
            && routines.iter().any(|r| reaches(s, r, call));
        let unread = s.keyword(call.at, "call") || inherited_locking.contains(callee);
        let self_bounded = !unplaced && reaches_self_bounded(call, raws, history);
        if !foreign && !resolved && unread && !self_bounded {
            // A routine that may clear the bound before it locks makes the
            // outside bound worthless. An unknown routine may do that too.
            let known = call
                .arity
                .map(|n| identity(&call.name, n))
                .is_some_and(|id| inherited_bounded.contains(&id));
            let may_clear = inherited.contains(callee) || unplaced || !known;
            let verb = if may_clear {
                UNREAD_CLEARING_CALL
            } else {
                UNREAD_CALL
            };
            raws.push(Raw::lock(call.at, verb, None));
        }
        if keeps(call, &clearing) {
            continue;
        }
        if s.toks[call.at].runs {
            timeouts.push((call.at, clear));
        } else {
            body_timeouts.push((call.at, clear));
        }
    }
    record_routines(s, &routines, &clearing, raws, history);
    // `timeout_in_force` reads the changes in token order.
    timeouts.sort_by_key(|(k, _)| *k);
    body_timeouts.sort_by_key(|(k, _)| *k);
}

/// Whether an unqualified `call` may reach a routine in another schema.
///
/// That holds after a `search_path` change. It also holds in the body of a
/// routine with its own `SET search_path` clause, which applies on each call.
fn unplaced_call(
    call: &Routine,
    path_change: Option<usize>,
    path_bodies: &[(usize, usize)],
) -> bool {
    !call.name.contains('.')
        && (path_change.is_some_and(|c| c <= call.at)
            || path_bodies
                .iter()
                .any(|&(from, to)| from <= call.at && call.at < to))
}

/// Whether `call` may reach the routine `r` that this file creates earlier.
///
/// The names and the argument count must match. A call that runs now reaches
/// only a routine that surely exists.
fn reaches(s: &Stmts, r: &Routine, call: &Routine) -> bool {
    r.at < call.at && (r.sure || !s.toks[call.at].runs) && r.name == call.name && r.accepts(call)
}

/// Each routine that the file creates.
fn file_routines(s: &Stmts) -> Vec<Routine> {
    let unconditional = unconditional(s);
    // The lint does not track which transaction a `ROLLBACK` ends. So it may
    // undo any earlier `CREATE`, which fails closed.
    let last_rollback = (0..s.toks.len())
        .rev()
        .find(|&k| s.starts[k] == k && (s.keyword(k, "rollback") || s.keyword(k, "abort")));
    (0..s.toks.len())
        .filter_map(|k| {
            let keyword = routine_keyword(s, k)?;
            let (name, open) = s.qualified_name(keyword + 1)?;
            let range = param_range(s, open, s.keyword(keyword, "procedure"));
            let foreign = language(s, k).is_some_and(|l| l != "plpgsql" && l != "sql");
            Some(Routine {
                name,
                arity: range.map(|(_, most)| most),
                min_arity: range.map(|(fewest, _)| fewest),
                at: k,
                foreign,
                sure: s.toks[k].runs && unconditional[k] && last_rollback.is_none_or(|r| r < k),
            })
        })
        .collect()
}

/// Teach the history which routines of this file clear the bound, run
/// foreign code or take a lock, for the calls in later migrations.
fn record_routines(
    s: &Stmts,
    routines: &[Routine],
    clearing: &BTreeSet<usize>,
    raws: &[Raw],
    history: &mut History,
) {
    for &i in clearing {
        history
            .clearing_routines
            .insert(base(&routines[i].name).to_string());
    }
    // A token belongs to the innermost routine body that holds it. So a lock
    // in a nested routine is not a lock of the routine around it.
    let bodies = routine_bodies(s);
    let body = |r: &Routine| {
        let at = r.at;
        let bodies = &bodies;
        routine_body(s, at)
            .map(move |(from, to)| (from..to).filter(move |&j| owns(s, bodies, at, j)))
            .into_iter()
            .flatten()
    };
    let mut locking = history.locking_routines.clone();
    for r in routines {
        let name = base(&r.name).to_string();
        if r.foreign {
            history.foreign_routines.insert(name.clone());
        }
        if body(r).any(|j| raws.iter().any(|raw| raw.at == j)) {
            locking.insert(name);
        }
    }
    // A resolved call adds no lock, so a routine that calls a locking routine
    // locks too. Repeat until no routine joins the set.
    let names: BTreeSet<String> = routines
        .iter()
        .map(|r| base(&r.name).to_string())
        .chain(locking.iter().cloned())
        .collect();
    let bases: BTreeSet<&str> = names.iter().map(String::as_str).collect();
    loop {
        let before = locking.len();
        for r in routines {
            let calls_locking = body(r).any(|j| {
                call_target(s, j, &bases).is_some_and(|c| locking.contains(base(&c.name)))
            });
            if calls_locking {
                locking.insert(base(&r.name).to_string());
            }
        }
        if locking.len() == before {
            break;
        }
    }
    // A routine that may not exist proves nothing about the routine a call reaches.
    for r in routines.iter().filter(|r| !r.foreign && r.sure) {
        let name = base(&r.name);
        // A call may pass any number of arguments from the required ones up
        // to all of them.
        if locking.contains(name)
            && !history.clearing_routines.contains(name)
            && let (Some(fewest), Some(most)) = (r.min_arity, r.arity)
        {
            for n in fewest..=most {
                history.bounded_routines.insert(identity(&r.name, n));
            }
        }
    }
    history.locking_routines = locking;
}

/// The full identity of a routine: its name as written, schema included, and
/// its number of parameters.
fn identity(name: &str, arity: usize) -> String {
    format!("{name}/{arity}")
}

/// Whether `call` names, by its exact identity, an earlier routine that bounds
/// its own locks. Unreadable code earlier in the file may replace the routine.
fn reaches_self_bounded(call: &Routine, raws: &[Raw], history: &History) -> bool {
    let opaque_before = raws
        .iter()
        .any(|raw| raw.at < call.at && [UNREADABLE_EXECUTE, FOREIGN_CODE].contains(&raw.verb));
    !opaque_before
        && !history.unbounded_routines.contains(base(&call.name))
        && call.arity.is_some_and(|n| {
            history
                .self_bounded_routines
                .contains(&identity(&call.name, n))
        })
}

/// Record which routines of this file bound every lock they take.
///
/// A routine counts only when each hot lock in its own body has a bound. Each
/// routine of this file that it calls must count too. An annotation does not
/// make a lock bounded. Code that the lint cannot read may replace any
/// routine, so it clears the record.
fn record_self_bounded(s: &Stmts, analysis: &Analysis, history: &mut History) {
    let routines = file_routines(s);
    let inside = |r: &Routine, j: usize| owns(s, &analysis.bodies, r.at, j);
    let names: BTreeSet<&str> = routines.iter().map(|r| base(&r.name)).collect();
    let mut bounded: Vec<bool> = routines
        .iter()
        .map(|r| {
            !r.foreign
                && r.arity.is_some()
                && !analysis
                    .hits
                    .iter()
                    .any(|hit| inside(r, hit.at) && needs_bound(analysis, hit))
        })
        .collect();
    // A call of a routine that may lock without a bound passes that lock up.
    loop {
        let unbounded: BTreeSet<&str> = routines
            .iter()
            .zip(&bounded)
            .filter(|(_, b)| !**b)
            .map(|(r, _)| base(&r.name))
            .collect();
        let mut changed = false;
        for (i, r) in routines.iter().enumerate() {
            let calls_unbounded = (0..s.toks.len())
                .filter(|&j| inside(r, j))
                .filter_map(|j| call_target(s, j, &names))
                .any(|c| unbounded.contains(base(&c.name)));
            if bounded[i] && calls_unbounded {
                bounded[i] = false;
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    for (r, _) in routines.iter().zip(&bounded).filter(|(_, b)| !**b) {
        history.unbounded_routines.insert(base(&r.name).to_string());
    }
    let unbounded = &history.unbounded_routines;
    history
        .self_bounded_routines
        .retain(|id| !unbounded.contains(base(id.split('/').next().unwrap_or(id))));
    for (r, _) in routines.iter().zip(&bounded).filter(|(_, b)| **b) {
        if !r.sure || unbounded.contains(base(&r.name)) {
            continue;
        }
        if let (Some(fewest), Some(most)) = (r.min_arity, r.arity) {
            for n in fewest..=most {
                history.self_bounded_routines.insert(identity(&r.name, n));
            }
        }
    }
    let opaque = analysis.hits.iter().any(|hit| {
        hit.body_start.is_none() && [UNREADABLE_EXECUTE, FOREIGN_CODE].contains(&hit.verb)
    });
    if opaque {
        history.self_bounded_routines.clear();
    }
}

/// The verb of a lock that a call of an unread routine may take.
const UNREAD_CALL: &str = "call of a routine the lint cannot read";

/// The verb of a lock that a call of an unread routine may take after it
/// clears the bound. No outside bound covers it.
const UNREAD_CLEARING_CALL: &str = "call of a routine that may clear lock_timeout";

/// The routine call at `k`, if any.
///
/// A `CALL` names any routine. Elsewhere, only a name whose last part is in
/// `bases` counts, and only when it is not part of DDL such as
/// `DROP FUNCTION f()`.
fn call_target(s: &Stmts, k: usize, bases: &BTreeSet<&str>) -> Option<Routine> {
    if s.starts[k] == k && s.keyword(k, "call") {
        let (name, open) = s.qualified_name(k + 1)?;
        let arity = arity(s, open);
        return Some(Routine {
            name,
            arity,
            min_arity: arity,
            at: k,
            foreign: false,
            sure: true,
        });
    }
    let callee = s.word(k)?;
    if !bases.contains(callee) || !s.is_punct(k + 1, '(') {
        return None;
    }
    let schema = (k >= 2 && s.is_punct(k - 1, '.'))
        .then(|| s.word(k - 2))
        .flatten();
    let first = if schema.is_some() { k - 2 } else { k };
    // The name after `CALL` belongs to the `CALL` above, so it is not a
    // second call.
    let not_a_call = first > 0
        && ["function", "procedure", "routine", "call"]
            .iter()
            .any(|w| s.keyword(first - 1, w));
    let name = schema.map_or_else(|| callee.to_string(), |schema| format!("{schema}.{callee}"));
    let arity = arity(s, k + 1);
    (!not_a_call).then_some(Routine {
        name,
        arity,
        min_arity: arity,
        at: k,
        foreign: false,
        sure: true,
    })
}

/// The fewest and the most arguments a call may pass to the routine whose
/// signature opens at `open`.
///
/// A parameter with a default may be left out. Postgres requires the defaults
/// to come last. A function never takes its `OUT` parameters as arguments,
/// but a `CALL` of a procedure passes them.
fn param_range(s: &Stmts, open: usize, procedure: bool) -> Option<(usize, usize)> {
    let close = closing_paren(s, open)?;
    let depth = s.toks[open].depth;
    let (mut fewest, mut most) = (0, 0);
    // The flags of the open parameter: seen, `OUT`, and with a default.
    let (mut seen, mut out, mut default) = (false, false, false);
    let mut parens = 0_usize;
    let mut brackets = 0_usize;
    let mut count = |seen: bool, out: bool, default: bool| {
        if seen && (procedure || !out) {
            most += 1;
            fewest += usize::from(!default);
        }
    };
    for j in (open..=close).filter(|&j| s.toks[j].depth == depth) {
        if s.is_punct(j, '(') {
            parens += 1;
            if parens == 1 {
                continue;
            }
        } else if s.is_punct(j, ')') {
            parens -= 1;
            if parens == 0 {
                count(seen, out, default);
                continue;
            }
        }
        if s.is_punct(j, '[') {
            brackets += 1;
        } else if s.is_punct(j, ']') {
            brackets = brackets.saturating_sub(1);
        }
        if parens == 1 && brackets == 0 && s.is_punct(j, ',') {
            count(seen, out, default);
            (seen, out, default) = (false, false, false);
            continue;
        }
        if parens == 1 {
            out |= !seen && s.keyword(j, "out");
            default |= s.keyword(j, "default") || s.is_punct(j, '=');
        }
        seen = true;
    }
    Some((fewest, most))
}

/// The number of comma-separated items in the parentheses that open at `open`.
fn arity(s: &Stmts, open: usize) -> Option<usize> {
    if !s.is_punct(open, '(') {
        return None;
    }
    let close = closing_paren(s, open)?;
    if close == open + 1 {
        return Some(0);
    }
    let depth = s.toks[open].depth;
    let mut parens = 0_usize;
    // A comma inside an `ARRAY[...]` constructor separates elements, not arguments.
    let mut brackets = 0_usize;
    let mut commas = 0;
    for j in (open..close).filter(|&j| s.toks[j].depth == depth) {
        if s.is_punct(j, '(') {
            parens += 1;
        } else if s.is_punct(j, ')') {
            parens -= 1;
        } else if s.is_punct(j, '[') {
            brackets += 1;
        } else if s.is_punct(j, ']') {
            brackets = brackets.saturating_sub(1);
        } else if s.is_punct(j, ',') && parens == 1 && brackets == 0 {
            commas += 1;
        }
    }
    Some(commas + 1)
}

/// The token range in which each new table is exempt.
///
/// The range starts at the `CREATE TABLE`. It ends at the first later
/// `DROP TABLE`, `ALTER TABLE ... RENAME TO` or `SET SCHEMA` of that name,
/// with or without a schema. It also ends at any later `DROP SCHEMA` or
/// `DROP OWNED`, or at any later `ROLLBACK`, which may undo the create. After
/// that the name can mean the hot table again. A `search_path` change ends
/// the range of an unqualified name for the same reason.
///
/// Any later `COMMIT` ends every range. After it, other sessions can see the
/// new table and lock it. So does a call of a routine from this file or an
/// earlier migration, or code that the lint cannot read.
fn new_table_spans(
    s: &Stmts,
    created: &BTreeMap<String, usize>,
    history: &History,
) -> BTreeMap<String, (usize, usize)> {
    let toks = s.toks;
    let mut ends: Vec<(SpanEnd, usize)> = Vec::new();
    // A routine from this file or an earlier migration may drop the table.
    let inherited = [
        &history.clearing_routines,
        &history.foreign_routines,
        &history.locking_routines,
    ];
    let bases: BTreeSet<&str> = (0..toks.len())
        .filter_map(|k| s.qualified_name(routine_keyword(s, k)? + 1))
        .filter_map(|(_, next)| s.word(next - 1))
        .chain(inherited.into_iter().flatten().map(String::as_str))
        .collect();
    for k in (0..toks.len()).filter(|&k| s.starts[k] == k) {
        let commit = s.keyword(k, "commit") || (s.keyword(k, "end") && toks[k].depth == 0);
        // `DROP SCHEMA` or `DROP OWNED` may drop any new table. The lint does
        // not track schemas or owners, so it ends every range.
        let drops_any =
            s.keyword(k, "drop") && (s.keyword(k + 1, "schema") || s.keyword(k + 1, "owned"));
        // After a commit, other sessions can see and lock the new table. A
        // call or unreadable code may drop or rename it.
        let opaque = calls_or_hides(s, k, &bases);
        if s.keyword(k, "rollback") || s.keyword(k, "abort") || drops_any || commit || opaque {
            ends.push((SpanEnd::All, k));
        } else if s.keyword(k, "drop") && s.keyword(k + 1, "table") {
            for name in s.name_list(s.skip_if_exists(k + 2)) {
                ends.push((SpanEnd::Name(name), k));
            }
        } else if s.keyword(k, "alter") && s.has_pair(k, "set", "schema") {
            // The new table moves away, so the name can mean the hot table.
            let end = s.statement_table(k).map_or(SpanEnd::All, SpanEnd::Name);
            ends.push((end, k));
        } else if s.keyword(k, "alter") && s.has_pair(k, "rename", "to") {
            // A rename of anything but a table, such as a schema, may move
            // every new table.
            let end = s.statement_table(k).map_or(SpanEnd::All, SpanEnd::Name);
            ends.push((end, k));
        } else if changes_search_path(s, k) {
            ends.push((SpanEnd::Unqualified, k));
        }
    }
    created
        .iter()
        .map(|(name, &from)| {
            let to = ends
                .iter()
                .filter(|(end, at)| *at > from && end.ends(name))
                .map(|(_, at)| *at)
                .min()
                .unwrap_or(usize::MAX);
            (name.clone(), (from, to))
        })
        .collect()
}

/// Whether the statement at `k` calls a routine or runs code the lint cannot
/// read. That is a `CALL`, a call of a routine in `bases`, an unreadable
/// `EXECUTE` or a `DO` body in another language.
fn calls_or_hides(s: &Stmts, k: usize, bases: &BTreeSet<&str>) -> bool {
    let calls = (k..s.end(k)).any(|j| call_target(s, j, bases).is_some());
    let execute =
        (k..s.end(k)).any(|j| dynamic_execute(s, j) && unreadable_execute(s, j).is_some());
    let foreign_do = s.keyword(k, "do") && language(s, k).is_some_and(|l| l != "plpgsql");
    calls || execute || foreign_do
}

/// What a statement ends in `new_table_spans`.
enum SpanEnd {
    /// Every new table, as after a `ROLLBACK`.
    All,
    /// Each new table with this name, whatever its schema.
    Name(String),
    /// Every new table without a schema in its name.
    Unqualified,
}

impl SpanEnd {
    fn ends(&self, name: &str) -> bool {
        match self {
            Self::All => true,
            // `scratch.t` and `t` may name the same table.
            Self::Name(n) => base(n) == base(name),
            Self::Unqualified => !name.contains('.'),
        }
    }
}

/// The first token that may change `search_path`, and record a textual
/// change in `history`.
///
/// An inherited change counts from token 0. Each test uses `c <= at`, and the
/// change token itself is a `SET`, so it holds no name or lock. Code that the
/// lint cannot read may change the path too, from the end of its statement
/// on. That doubt holds for the rest of the file only. Carried into history,
/// one such call would leave every later migration unreadable.
fn path_change(s: &Stmts, opaque: &[usize], history: &mut History) -> Option<usize> {
    let text = (0..s.toks.len())
        .find(|&k| s.starts[k] == k && running_path_change(s, k, true))
        .into_iter()
        .chain(path_routine_call(s, history))
        .min();
    let hidden = opaque.first().map(|&k| s.end(k));
    // Only a session change that runs now carries into history. A local
    // change ends with its transaction. A routine body runs only when called,
    // and that call counts as code the lint cannot read in its own file.
    let session = (0..s.toks.len()).any(|k| s.starts[k] == k && running_path_change(s, k, false));
    history.search_path_changed |= session;
    let local = text.into_iter().chain(hidden).min();
    local.or_else(|| history.search_path_changed.then_some(0))
}

/// Each running token where code that the lint cannot read runs.
///
/// That is a `DO` in another language, an unreadable `EXECUTE`, or a call
/// that no earlier routine in this file matches. Such code may change any
/// session setting, such as `search_path` or `standard_conforming_strings`.
fn opaque_points(s: &Stmts, history: &History) -> Vec<usize> {
    let routines = file_routines(s);
    let inherited = [
        &history.clearing_routines,
        &history.foreign_routines,
        &history.locking_routines,
    ];
    let bases: BTreeSet<&str> = routines
        .iter()
        .map(|r| base(&r.name))
        .chain(inherited.into_iter().flatten().map(String::as_str))
        .collect();
    (0..s.toks.len())
        .filter(|&k| s.toks[k].runs && opaque_at(s, k, &routines, &bases))
        .collect()
}

/// Whether the token at `k` runs code that the lint cannot read.
///
/// That is a `DO` in another language, an unreadable `EXECUTE`, or a call that
/// no earlier routine of this file reaches. `bases` names the routines whose
/// calls the lint looks for.
fn opaque_at(s: &Stmts, k: usize, routines: &[Routine], bases: &BTreeSet<&str>) -> bool {
    let start = s.starts[k] == k;
    let execute = dynamic_execute(s, k) && unreadable_execute(s, k).is_some();
    let resolved = |call: &Routine| routines.iter().any(|r| !r.foreign && reaches(s, r, call));
    let call = call_target(s, k, bases).is_some_and(|c| !resolved(&c));
    (start && foreign_do(s, k)) || execute || call
}

/// Whether the statement at `k` holds a `search_path` change that runs now.
///
/// The test reads each setter token, not the statement around it. So a setter
/// in an uncalled routine body does not count. Unless `local` is set, nor does
/// a `SET LOCAL` or a plain `set_config` with a literal `true` scope.
fn running_path_change(s: &Stmts, k: usize, local: bool) -> bool {
    let scope = usize::from(s.keyword(k + 1, "local") || s.keyword(k + 1, "session"));
    let set = s.keyword(k, "set") && path_setting(s, k + 1 + scope);
    let reset = s.keyword(k, "reset") && (path_setting(s, k + 1) || s.keyword(k + 1, "all"));
    if set || reset {
        return s.toks[k].runs && (local || !(set && s.keyword(k + 1, "local")));
    }
    let literal = |j: usize| s.word(j).or_else(|| s.string(j));
    let is_local = |j: usize| {
        s.string(j + 2).is_some()
            && s.is_punct(j + 3, ',')
            && s.is_punct(j + 5, ',')
            && literal(j + 6).is_some_and(pg_true)
    };
    (k..s.end(k)).any(|j| path_call(s, j) && s.toks[j].runs && (local || !is_local(j)))
}

/// The end of the first running call of a routine whose body may change
/// `search_path`. The change takes effect when that call returns.
///
/// A routine that calls such a routine may change the path too. The set grows
/// until no routine joins it, as for clearing and locking routines. The
/// history keeps the set for later migrations.
fn path_routine_call(s: &Stmts, history: &mut History) -> Option<usize> {
    let routines = file_routines(s);
    let bodies = routine_bodies(s);
    let own = |r: &Routine, j: usize| owns(s, &bodies, r.at, j);
    let inherited = history.path_routines.clone();
    let all: BTreeSet<&str> = routines
        .iter()
        .map(|r| base(&r.name))
        .chain(inherited.iter().map(String::as_str))
        .collect();
    // A body that runs code the lint cannot read may change the path too.
    let watched: BTreeSet<&str> = [
        &history.clearing_routines,
        &history.foreign_routines,
        &history.locking_routines,
    ]
    .into_iter()
    .flatten()
    .map(String::as_str)
    .chain(routines.iter().map(|r| base(&r.name)))
    .collect();
    let changes = |j: usize| {
        (s.starts[j] == j && changes_search_path(s, j)) || opaque_at(s, j, &routines, &watched)
    };
    let mut names: BTreeSet<&str> = routines
        .iter()
        .filter(|r| (r.at..s.toks.len()).any(|j| own(r, j) && changes(j)))
        .map(|r| base(&r.name))
        .chain(inherited.iter().map(String::as_str))
        .collect();
    loop {
        let before = names.len();
        for r in &routines {
            let calls = (r.at..s.toks.len()).any(|j| {
                own(r, j) && call_target(s, j, &all).is_some_and(|c| names.contains(base(&c.name)))
            });
            if calls {
                names.insert(base(&r.name));
            }
        }
        if names.len() == before {
            break;
        }
    }
    history
        .path_routines
        .extend(names.iter().map(ToString::to_string));
    (0..s.toks.len())
        .filter(|&k| s.toks[k].runs)
        .find(|&k| call_target(s, k, &names).is_some())
        .map(|k| s.end(k))
}

/// Whether the token at `k` is a `set_config` call that may name
/// `search_path`: it names it, or its name is not one plain literal.
fn path_call(s: &Stmts, k: usize) -> bool {
    s.is(k, "set_config")
        && s.is_punct(k + 1, '(')
        && (!(s.string(k + 2).is_some() && s.is_punct(k + 3, ','))
            || s.string(k + 2).is_some_and(|v| {
                ["search_path", "role", "session_authorization"]
                    .iter()
                    .any(|name| v.trim().eq_ignore_ascii_case(name))
            }))
}

/// Whether the setting that a `SET` or `RESET` names at `j` may change where
/// an unqualified name goes.
///
/// `SET SCHEMA` is an alias of `SET search_path`. The `"$user"` entry of the
/// path follows the current role, so a role change counts too.
fn path_setting(s: &Stmts, j: usize) -> bool {
    s.is(j, "search_path")
        || s.keyword(j, "schema")
        || s.keyword(j, "role")
        || s.keyword(j, "authorization")
        || s.is(j, "session_authorization")
        || (s.keyword(j, "session") && s.keyword(j + 1, "authorization"))
}

/// Whether the statement at `k` may change `search_path`.
///
/// `SET`, `RESET` and a `set_config` call count, wherever they sit.
fn changes_search_path(s: &Stmts, k: usize) -> bool {
    let scope = usize::from(s.keyword(k + 1, "local") || s.keyword(k + 1, "session"));
    let set = s.keyword(k, "set") && path_setting(s, k + 1 + scope);
    let reset = s.keyword(k, "reset") && (path_setting(s, k + 1) || s.keyword(k + 1, "all"));
    // A call counts when it names `search_path`, or when its name is not one
    // plain literal, which may be `search_path` too.
    let call = (k..s.end(k)).any(|j| path_call(s, j));
    set || reset || call
}

/// Resolve each statement's table against the history, in source order.
///
/// A `CREATE INDEX` that surely runs teaches the history its table.
fn resolve(
    mut raws: Vec<Raw>,
    s: &Stmts,
    unconditional: &[bool],
    new_tables: &BTreeMap<String, (usize, usize)>,
    path_change: Option<usize>,
    history: &mut History,
) -> Vec<Hit> {
    let toks = s.toks;
    raws.sort_by_key(|raw| raw.at);
    // After a `search_path` change, the lint cannot tell the schema of an
    // unqualified name. Such a name is never placed or learnt.
    let unplaced =
        |name: &str, at: usize| !name.contains('.') && path_change.is_some_and(|c| c <= at);
    let bodies = routine_bodies(s);
    let mut hits = Vec::with_capacity(raws.len());
    for raw in raws {
        // A `format()` placeholder makes a name unknown, which fails closed.
        let placeholder = raw.table.as_deref().is_some_and(|t| t.contains('%'))
            || raw.index.as_deref().is_some_and(|i| i.contains('%'));
        let table = raw.table.filter(|_| !placeholder).or_else(|| {
            raw.index
                .as_deref()
                .filter(|i| !placeholder && !unplaced(i, raw.at))
                .and_then(|i| history.index_table(i))
        });
        // The history only grows, so a later build of the same name cannot
        // hide a hot table. A cold table is learnt only from a build that
        // surely runs: not conditional, and not `IF NOT EXISTS`, which may do
        // nothing. A hot table is always learnt, because it can only make a
        // later drop stricter.
        let sure = toks[raw.at].runs
            && unconditional[raw.at]
            && !s.has_pair(raw.at, "not", "exists")
            && !table.as_deref().is_some_and(|t| unplaced(t, raw.at));
        let learn = sure || table.as_deref().is_some_and(|t| history.is_hot(t));
        if let (Some(index), Some(table), "CREATE INDEX", true) =
            (&raw.index, &table, raw.verb, learn)
        {
            history
                .indexes
                .entry(index_key(table, index))
                .or_default()
                .insert(table.clone());
        }
        // A rename or schema move takes the history to the new name. When it
        // surely runs, the old name is forgotten.
        if let (Some(index), "ALTER INDEX") = (&raw.index, raw.verb)
            && !unplaced(index, raw.at)
            && let Some(new_key) = moved_index_key(s, raw.at, index)
        {
            let old_key = index_key(index, index);
            let tables = history.indexes.get(&old_key).cloned();
            if toks[raw.at].runs && unconditional[raw.at] {
                history.indexes.remove(&old_key);
            }
            if let Some(tables) = tables {
                history.indexes.entry(new_key).or_default().extend(tables);
            }
        }
        // A drop that surely runs removes the name. A later index of that name
        // is then unknown, which fails closed. A table drop takes every index
        // on the table with it.
        if toks[raw.at].runs && unconditional[raw.at] {
            if let (Some(index), "DROP INDEX") = (&raw.index, raw.verb)
                && !unplaced(index, raw.at)
            {
                history.indexes.remove(&index_key(index, index));
            }
            if let (Some(table), "DROP TABLE") = (&table, raw.verb) {
                history.forget_table(table);
            }
            if matches!(
                raw.verb,
                "DROP SCHEMA ... CASCADE" | "DROP OWNED" | "DROP ... CASCADE"
            ) {
                history.forget_cold_indexes();
            }
            // A table moves its indexes with it to the new schema. A dropped
            // column or constraint can take indexes with it.
            if let (Some(table), "ALTER TABLE") = (&table, raw.verb)
                && (s.has_pair(raw.at, "set", "schema") || drops_dependents(s, raw.at))
            {
                history.forget_table(table);
            }
        }
        let hot = table.as_deref().is_none_or(|t| {
            history.is_hot(t)
                && new_tables
                    .get(t)
                    .is_none_or(|(from, to)| raw.at < *from || raw.at > *to)
        });
        hits.push(Hit {
            at: raw.at,
            line: toks[raw.at].line,
            verb: raw.verb,
            index: raw.index,
            table,
            kind: raw.kind,
            in_body: toks[raw.at].depth > 0,
            body_start: (!toks[raw.at].runs).then(|| body_start(&bodies, toks, raw.at)),
            hot,
        });
    }

    hits
}

/// The first token and the end of each routine body.
///
/// A body is the code after `AS`, a `BEGIN ATOMIC` block or a `RETURN`
/// expression. A routine that another body creates has a body of its own.
fn routine_bodies(s: &Stmts) -> Vec<(usize, usize)> {
    (0..s.toks.len())
        .filter_map(|k| routine_body(s, k))
        .collect()
}

/// Blank the body of each routine or `DO` in another language.
///
/// Postgres only stores the source of such a routine, so no text in it is SQL:
/// it takes no lock and changes no setting. A call of the routine counts as
/// foreign code instead, and a foreign `DO` counts as one lock at the `DO`.
/// Only tokens deeper than the statement go, so a trailing `LANGUAGE` clause
/// stays.
fn blank_foreign_bodies(toks: &mut [Token]) {
    let ranges: Vec<(usize, usize, usize)> = {
        let s = Stmts::new(toks);
        (0..toks.len())
            .filter(|&k| language(&s, k).is_some_and(|l| l != "plpgsql" && l != "sql"))
            .filter_map(|k| {
                let (from, to) =
                    routine_body(&s, k).or_else(|| foreign_do(&s, k).then(|| (k + 1, s.end(k))))?;
                Some((from, to, toks[k].depth))
            })
            .collect()
    };
    for (from, to, depth) in ranges {
        for j in from..to {
            // The lexer reads a quoted `LANGUAGE 'name'` in a `DO` as body
            // text, but it names the language.
            let names = j > 0 && toks[j - 1].depth == depth && is_keyword(&toks[j - 1], "language");
            if toks[j].depth <= depth || names {
                continue;
            }
            toks[j].tok = Tok::Punct(' ');
            toks[j].quoted = false;
        }
    }
}

/// The first token and the end of the body of the routine that `CREATE`
/// starts at `k`, if `k` starts one.
fn routine_body(s: &Stmts, k: usize) -> Option<(usize, usize)> {
    routine_keyword(s, k)?;
    let depth = s.toks[k].depth;
    let end = s.end(k);
    let atomic = |j: usize| s.keyword(j, "begin") && s.keyword(j + 1, "atomic");
    let start = (k + 1..end).find(|&j| {
        let inline = s.keyword(j, "return") || atomic(j);
        s.toks[j].depth > depth || (s.toks[j].depth == depth && !s.toks[j].runs && inline)
    })?;
    // An atomic body holds statements of its own, so it runs to its `END`.
    let end = if atomic(start) {
        atomic_end(s, start) + 1
    } else {
        end
    };
    Some((start, end))
}

/// Whether the token at `at` belongs to the body of the routine at `k`.
///
/// `k` is the `CREATE` of the routine. A token in a routine nested in that
/// body belongs to the nested routine instead.
fn owns(s: &Stmts, bodies: &[(usize, usize)], k: usize, at: usize) -> bool {
    routine_body(s, k).is_some_and(|(from, to)| {
        let innermost = bodies
            .iter()
            .filter(|&&(start, end)| start <= at && at < end)
            .map(|&(start, _)| start)
            .max();
        from <= at && at < to && innermost == Some(from)
    })
}

/// The `END` that closes the `BEGIN ATOMIC` at `begin`.
///
/// A SQL `CASE` expression also ends with `END`, so the search counts them.
fn atomic_end(s: &Stmts, begin: usize) -> usize {
    let depth = s.toks[begin].depth;
    let mut cases = 0_usize;
    for j in (begin + 2..s.toks.len()).filter(|&j| s.toks[j].depth == depth) {
        if s.keyword(j, "case") {
            cases += 1;
        } else if s.keyword(j, "end") {
            if cases == 0 {
                return j;
            }
            cases -= 1;
        }
    }
    s.toks.len().saturating_sub(1)
}

/// The first token of the innermost routine body that holds the token at `at`.
///
/// A bound from an outer body does not hold when an inner routine runs. The
/// SQL of an `EXECUTE` in a body belongs to that body. Code that runs later
/// outside any routine falls back to its run of deferred tokens.
fn body_start(bodies: &[(usize, usize)], toks: &[Token], at: usize) -> usize {
    bodies
        .iter()
        .filter(|&&(start, end)| start <= at && at < end)
        .map(|&(start, _)| start)
        .max()
        .unwrap_or_else(|| {
            (0..at)
                .rev()
                .take_while(|&j| !toks[j].runs)
                .last()
                .unwrap_or(at)
        })
}

/// Whether each token runs on every path through its `DO` body.
///
/// A token in nested SQL surely runs only when its enclosing statement does.
///
/// A token inside an `IF`, `CASE` or `LOOP`, or after a `RETURN`, `EXIT` or
/// `CONTINUE`, may not run. Nothing in a body with an `EXCEPTION` handler surely runs, because
/// the handler rolls the block back. A `CASE` expression that ends in a bare `END` leaves the rest of the
/// body conditional, which fails closed. A top-level token always runs.
/// Nothing in a `DO` body in another language surely runs.
fn unconditional(s: &Stmts) -> Vec<bool> {
    let toks = s.toks;
    let handler = |j: usize| s.keyword(j, "exception") && !(j > 0 && s.keyword(j - 1, "raise"));
    // A block with an exception handler runs as a subtransaction. An error
    // rolls the whole block back, so nothing in that block surely happens.
    let mut rolled_back = vec![false; toks.len()];
    let mut from = 0;
    while from < toks.len() {
        if toks[from].depth == 0 {
            from += 1;
            continue;
        }
        let to = (from..toks.len())
            .find(|&j| toks[j].depth == 0)
            .unwrap_or(toks.len());
        if (from..to).any(handler) {
            // When the blocks do not pair up, the whole body fails closed.
            match handled_blocks(s, from, to) {
                Some(blocks) => {
                    for (begin, end) in blocks {
                        rolled_back[begin..=end].fill(true);
                    }
                }
                None => rolled_back[from..to].fill(true),
            }
        }
        from = to;
    }

    // Branch state per dollar-quote depth. A nested string must not reset
    // the state of the body around it.
    let mut state = vec![(0_usize, false)];
    let mut depth = 0;
    let mut out = Vec::with_capacity(toks.len());
    for (k, tok) in toks.iter().enumerate() {
        if tok.depth > depth {
            state.truncate(depth + 1);
        }
        depth = tok.depth;
        if state.len() <= depth {
            state.resize(depth + 1, (0, false));
        }
        let (branches, skippable) = &mut state[depth];
        let after_end = k > 0 && s.keyword(k - 1, "end");
        // A quoted word, such as the label in `END "if"`, is never a keyword.
        match s.word(k).filter(|_| !tok.quoted) {
            Some("end") if ["if", "loop", "case"].iter().any(|w| s.keyword(k + 1, w)) => {
                *branches = branches.saturating_sub(1);
            }
            // A control-flow `IF` starts a statement. The `IF [NOT] EXISTS` of
            // a DDL statement sits in the middle of one.
            Some("if") if !after_end && s.starts[k] == k => {
                *branches += 1;
            }
            Some("loop" | "case") if !after_end => *branches += 1,
            // An early exit, or a handler, makes the rest of the body conditional.
            // `RETURN NEXT` and `RETURN QUERY` add rows but do not exit.
            Some("return") if s.keyword(k + 1, "next") || s.keyword(k + 1, "query") => {}
            Some("return" | "exit" | "continue") => *skippable = true,
            Some("exception") if handler(k) => *skippable = true,
            _ => {}
        }
        let here = *branches == 0 && !*skippable && !rolled_back[k];
        // Nested SQL, such as the SQL of an `EXECUTE`, runs only when the
        // statement around it runs. Depth 0 always runs.
        let around = state
            .get(1..depth)
            .is_none_or(|outer| outer.iter().all(|&(b, skip)| b == 0 && !skip));
        out.push(depth == 0 || (here && around));
    }
    // The lint cannot read a body in another language, so nothing in it
    // surely runs. A lock in it still counts, which fails closed.
    for k in (0..toks.len()).filter(|&k| foreign_do(s, k)) {
        let end = s.end(k).min(toks.len());
        out[k + 1..end].fill(false);
    }
    out
}

/// Each `BEGIN ... END` block in tokens `from..to` that has an exception
/// handler, as a token range.
///
/// `END IF`, `END LOOP` and `END CASE` close no block, and nor does the `END`
/// of a SQL `CASE` expression. `None` means the blocks do not pair up.
fn handled_blocks(s: &Stmts, from: usize, to: usize) -> Option<Vec<(usize, usize)>> {
    let mut open: Vec<(usize, bool)> = Vec::new();
    let mut expression_cases = 0_usize;
    let mut blocks = Vec::new();
    for j in from..to {
        match s.word(j).filter(|_| !s.toks[j].quoted) {
            Some("begin") => open.push((j, false)),
            Some("exception") if !(j > 0 && s.keyword(j - 1, "raise")) => {
                open.last_mut()?.1 = true;
            }
            Some("case") if s.starts[j] != j => expression_cases += 1,
            Some("end") if ["if", "loop", "case"].iter().any(|w| s.keyword(j + 1, w)) => {}
            Some("end") if expression_cases > 0 => expression_cases -= 1,
            Some("end") => {
                let (begin, handled) = open.pop()?;
                if handled {
                    blocks.push((begin, j));
                }
            }
            _ => {}
        }
    }
    open.is_empty().then_some(blocks)
}

/// The `lock_timeout` change at token `k`, if any: whether it sets a bound.
///
/// Matches `SET [LOCAL | SESSION] lock_timeout {= | TO} <value>`,
/// `RESET lock_timeout`, `RESET ALL` and `set_config('lock_timeout', ...)`.
/// `SET` and `RESET` count only at the start of a statement, so
/// `ALTER ROLE ... SET` does not.
fn timeout_change(s: &Stmts, k: usize) -> Option<Timeout> {
    let start = s.starts[k] == k;
    match s.word(k)? {
        "set" if start => {
            let mut j = k + 1;
            let local = s.keyword(j, "local");
            if local || s.keyword(j, "session") {
                j += 1;
            }
            if !s.is(j, "lock_timeout") {
                return None;
            }
            j += 1;
            if s.is_punct(j, '=') || s.keyword(j, "to") {
                j += 1;
            }
            Some(Timeout::Set {
                bounds: bounds_wait(s, j),
                local,
            })
        }
        "reset" if start && (s.is(k + 1, "lock_timeout") || s.keyword(k + 1, "all")) => {
            Some(Timeout::Set {
                bounds: false,
                local: false,
            })
        }
        // A `DO` body or a procedure may end its transaction too. Inside
        // PL/pgSQL, `END` closes a block, so it counts only at top level.
        "end" if start && s.toks[k].depth == 0 => Some(Timeout::Commit),
        "commit" if start => Some(Timeout::Commit),
        "rollback" | "abort" if start => {
            // `ROLLBACK [WORK | TRANSACTION] TO [SAVEPOINT] s` restores the value
            // from the savepoint, which the lint does not track.
            if (k + 1..=k + 2).any(|j| s.keyword(j, "to")) {
                Some(Timeout::RollbackToSavepoint)
            } else {
                Some(Timeout::Rollback)
            }
        }
        // A named-argument call never sets a bound. When it may name
        // `lock_timeout`, it counts as a session clear, which fails closed.
        "set_config" if s.is_punct(k + 1, '(') && lock_timeout_call(s, k + 1) == Some(true) => {
            Some(Timeout::Set {
                bounds: false,
                local: false,
            })
        }
        "set_config"
            if s.is_punct(k + 1, '(')
                && s.string(k + 2)
                    .is_some_and(|v| v.trim().eq_ignore_ascii_case("lock_timeout"))
                && s.is_punct(k + 3, ',') =>
        {
            // A query runs the function once per row, so a filter can skip it.
            // A bound counts only from a bare `SELECT` or `PERFORM` of the
            // call. A clear counts anywhere.
            let bounds = bounds_wait(s, k + 4);
            // A bound is session-level only with a plain false scope, and a
            // clear is local only with a plain true one. Any other scope takes
            // the stricter reading.
            let scope = s
                .word(k + 6)
                .or_else(|| s.string(k + 6))
                .filter(|_| s.is_punct(k + 7, ')'));
            let local = if bounds {
                !scope.is_some_and(pg_false)
            } else {
                scope.is_some_and(pg_true)
            };
            (!bounds || s.is_bare_call(k)).then_some(Timeout::Set { bounds, local })
        }
        // Any other call that names `lock_timeout`, such as one with a cast,
        // counts as a session clear. That fails closed.
        "set_config" if s.is_punct(k + 1, '(') && lock_timeout_call(s, k + 1).is_some() => {
            Some(Timeout::Set {
                bounds: false,
                local: false,
            })
        }
        // A name that is not one plain literal may still be `lock_timeout`.
        "set_config"
            if s.is_punct(k + 1, '(') && !(s.string(k + 2).is_some() && s.is_punct(k + 3, ',')) =>
        {
            Some(Timeout::Set {
                bounds: false,
                local: false,
            })
        }
        _ => None,
    }
}

/// Whether the call whose `(` is at `open` names `lock_timeout` in a string.
///
/// `Some(true)` means the call also uses named arguments. `None` means it
/// does not name the setting.
fn lock_timeout_call(s: &Stmts, open: usize) -> Option<bool> {
    let mut parens = 0_usize;
    let mut named = false;
    let mut names_it = false;
    for j in open..s.toks.len() {
        if s.is_punct(j, '(') {
            parens += 1;
        } else if s.is_punct(j, ')') {
            parens -= 1;
            if parens == 0 {
                break;
            }
        }
        named |= (s.is_punct(j, '=') && s.is_punct(j + 1, '>'))
            || (s.is_punct(j, ':') && s.is_punct(j + 1, '='));
        names_it |= s
            .string(j)
            .is_some_and(|v| v.trim().eq_ignore_ascii_case("lock_timeout"));
    }
    names_it.then_some(named)
}

/// Whether Postgres reads `value` as boolean true.
///
/// Postgres trims the value and ignores case. It accepts any unique prefix of
/// `true`, `yes` or `on`, and `1`. `o` alone is ambiguous, so it fails.
fn pg_true(value: &str) -> bool {
    let v = value.trim().to_ascii_lowercase();
    !v.is_empty()
        && ("true".starts_with(&v)
            || "yes".starts_with(&v)
            || (v.len() > 1 && "on".starts_with(&v))
            || v == "1")
}

/// Whether Postgres reads `value` as boolean false.
///
/// Postgres trims the value and ignores case. It accepts any unique prefix of
/// `false`, `no` or `off`, and `0`. `o` alone is ambiguous, so it fails.
fn pg_false(value: &str) -> bool {
    let v = value.trim().to_ascii_lowercase();
    !v.is_empty()
        && ("false".starts_with(&v)
            || "no".starts_with(&v)
            || (v.len() > 1 && "off".starts_with(&v))
            || v == "0")
}

/// `CREATE INDEX`, `CREATE TABLE`, and the trigger, rule and policy forms.
fn create(s: &Stmts, k: usize, raws: &mut Vec<Raw>, created: &mut BTreeMap<String, usize>) {
    let mut j = k + 1;
    if s.keyword(j, "or") && s.keyword(j + 1, "replace") {
        j += 2;
    }
    if s.keyword(j, "unique") {
        j += 1;
    }
    if s.keyword(j, "index") {
        raws.extend(create_index(s, k, j + 1));
        return;
    }
    if s.keyword(j, "constraint") {
        j += 1;
    }
    let target = match s.word(j).filter(|_| !s.toks[j].quoted) {
        Some("trigger") => Some(("CREATE TRIGGER", "on")),
        Some("policy") => Some(("CREATE POLICY", "on")),
        Some("rule") => Some(("CREATE RULE", "to")),
        _ => None,
    };
    if let Some((verb, keyword)) = target {
        raws.push(Raw::lock(k, verb, s.name_after(j, keyword)));
        return;
    }
    // A temporary table never counts. `ON COMMIT DROP` or the session end
    // drops it, and the name then means the hot table again.
    let temporary = (k + 1..=k + 2).any(|j| s.keyword(j, "temp") || s.keyword(j, "temporary"));
    if let Some(table) = s.statement_table(k) {
        let guarded = s.has_pair(k, "not", "exists");
        if !guarded && !temporary {
            created.entry(table).or_insert(k);
        }
    }
}

/// `CREATE [UNIQUE] INDEX [CONCURRENTLY] [IF NOT EXISTS] [name] ON [ONLY] table`.
///
/// `j` is the token after `INDEX`.
fn create_index(s: &Stmts, k: usize, mut j: usize) -> Option<Raw> {
    let concurrent = s.keyword(j, "concurrently");
    j = s.skip_if_exists(j + usize::from(concurrent));
    let mut index = None;
    if !s.keyword(j, "on") {
        let (name, next) = s.qualified_name(j)?;
        index = Some(name);
        j = next;
    }
    if !s.keyword(j, "on") {
        return None;
    }
    j += 1;
    if s.keyword(j, "only") {
        j += 1;
    }
    let (table, _) = s.qualified_name(j)?;
    Some(Raw {
        at: k,
        verb: "CREATE INDEX",
        index,
        table: Some(table),
        kind: Kind::Index { concurrent },
    })
}

/// The `DROP` forms that lock a table.
///
/// Dropping a table also drops the foreign-key triggers on each table it
/// references. That takes ACCESS EXCLUSIVE on the referenced tables.
fn drop(s: &Stmts, k: usize, history: &History, raws: &mut Vec<Raw>) {
    match s.word(k + 1).filter(|_| !s.toks[k + 1].quoted) {
        Some("index") => {
            let concurrent = s.keyword(k + 2, "concurrently");
            let j = s.skip_if_exists(k + 2 + usize::from(concurrent));
            raws.extend(s.name_list(j).into_iter().map(|index| Raw {
                at: k,
                verb: "DROP INDEX",
                index: Some(index),
                table: None,
                kind: Kind::Index { concurrent },
            }));
        }
        Some("table") => {
            for table in s.name_list(s.skip_if_exists(k + 2)) {
                raws.extend(
                    referenced(history, &table)
                        .map(|t| Raw::lock(k, "DROP TABLE (foreign key)", Some(t))),
                );
                raws.push(Raw::lock(k, "DROP TABLE", Some(table)));
            }
            // CASCADE also drops the foreign keys of every referencing table.
            if (k..s.end(k)).any(|j| s.keyword(j, "cascade")) {
                raws.push(Raw::lock(k, "DROP TABLE ... CASCADE", None));
            }
        }
        // Each drops every table it reaches, hot ones included.
        Some("schema") if (k..s.end(k)).any(|j| s.keyword(j, "cascade")) => {
            raws.push(Raw::lock(k, "DROP SCHEMA ... CASCADE", None));
        }
        Some("owned") => raws.push(Raw::lock(k, "DROP OWNED", None)),
        Some("trigger") => raws.push(Raw::lock(k, "DROP TRIGGER", s.name_after(k + 2, "on"))),
        Some("policy") => raws.push(Raw::lock(k, "DROP POLICY", s.name_after(k + 2, "on"))),
        Some("rule") => raws.push(Raw::lock(k, "DROP RULE", s.name_after(k + 2, "on"))),
        _ => {}
    }
    // Any other CASCADE may drop a column, default or trigger on a hot table,
    // as `DROP TYPE ... CASCADE` does.
    let cascade = (k..s.end(k)).any(|j| s.keyword(j, "cascade"));
    if cascade && !matches!(s.word(k + 1), Some("table" | "schema" | "owned")) {
        raws.push(Raw::lock(k, "DROP ... CASCADE", None));
    }
}

/// `ALTER TABLE old RENAME TO new` carries the foreign keys and indexes of
/// `old` to `new`.
///
/// The old name keeps them too. Remembering a key that moved fails closed.
fn rename(s: &Stmts, k: usize, history: &mut History) {
    let start = s.starts[k];
    if !(s.keyword(start, "alter") && s.keyword(start + 1, "table")) {
        return;
    }
    let (Some(old), Some((new, _))) = (s.statement_table(start), s.qualified_name(k + 2)) else {
        return;
    };
    let keys = history
        .references
        .get(base(&old))
        .cloned()
        .unwrap_or_default();
    history
        .references
        .entry(base(&new).to_string())
        .or_default()
        .extend(keys);
    for names in [&mut history.partitions, &mut history.partitioned] {
        if names.contains(base(&old)) {
            names.insert(base(&new).to_string());
        }
    }
    // An index on the old name now sits on the new one. A foreign key that
    // pointed at the old name now points at the new one.
    for tables in history
        .indexes
        .values_mut()
        .chain(history.references.values_mut())
    {
        if tables.iter().any(|t| base(t) == base(&old)) {
            tables.insert(new.clone());
        }
    }
}

/// Whether the `ALTER TABLE` at `k` may drop or rebuild a dependent object,
/// such as a foreign key or an index.
///
/// `DROP CONSTRAINT` drops one. `DROP [COLUMN]` drops the keys and indexes on
/// the column. A column type change rebuilds them. A key change also changes
/// the key's triggers on the referenced table.
fn drops_dependents(s: &Stmts, k: usize) -> bool {
    const KEEPS_KEYS: [&str; 4] = ["default", "not", "identity", "expression"];
    (k..s.end(k)).any(|j| {
        let drops = s.keyword(j, "drop") && !KEEPS_KEYS.iter().any(|w| s.keyword(j + 1, w));
        let retypes = s.keyword(j, "type")
            && (s.keyword(j - 1, "data")
                || s.keyword(j - 2, "column")
                || s.keyword(j - 2, "alter"));
        drops || retypes
    })
}

/// A lock on an unknown table for the PL/pgSQL `EXECUTE` at `k`, when the
/// lint cannot read the SQL it runs.
///
/// The lint reads only constant SQL: one literal, or `format()` of one
/// literal. Only `INTO` or `USING` may follow. A variable, a composed
/// expression, or a `format()` `%s` placeholder may hold any statement.
fn unreadable_execute(s: &Stmts, k: usize) -> Option<Raw> {
    let toks = s.toks;
    let depth = toks[k].depth;
    let end = s.end(k);
    let formatted = s.is(k + 1, "format") && s.is_punct(k + 2, '(');
    let literal = if formatted { k + 3 } else { k + 1 };
    let constant = toks.get(literal).is_some_and(|t| t.depth > depth);
    // The first token after the SQL, at the depth of the statement.
    let after_literal = (literal..end).find(|&j| toks[j].depth == depth);
    // The template of `format()` must be the literal alone, so a `,` or the
    // closing `)` follows it. A composed template such as `'a' || 'b'` is
    // unreadable.
    let rest = if formatted {
        closing_paren(s, k + 2)
            .filter(|&close| after_literal.is_some_and(|j| j == close || s.is_punct(j, ',')))
            .map(|close| close + 1)
    } else {
        after_literal.or(Some(end))
    };
    // `FOR ... IN EXECUTE` ends with the `LOOP` of its body.
    let tail_ok = rest.is_some_and(|j| {
        j >= end || s.keyword(j, "into") || s.keyword(j, "using") || s.keyword(j, "loop")
    });
    let text_placeholder = (k + 1..end).any(|j| toks[j].depth > depth && s.is(j, "%s"));
    (!constant || !tail_ok || text_placeholder).then(|| Raw::lock(k, UNREADABLE_EXECUTE, None))
}

/// The `lock_timeout` change at `k`, as `analyse` records it.
///
/// A conditional setter cannot set a bound, but a conditional clear may end
/// one. After a `search_path` change, an unqualified `set_config` may call a
/// function that shadows the built-in. So it counts as a session clear.
fn recorded_change(
    s: &Stmts,
    k: usize,
    unconditional: bool,
    (path_change, path_bodies): (Option<usize>, &[(usize, usize)]),
) -> Option<Timeout> {
    let change = timeout_change(s, k)?;
    let qualified = k >= 2 && s.is_punct(k - 1, '.') && s.is(k - 2, "pg_catalog");
    let changed = path_change.is_some_and(|c| c <= k)
        || path_bodies.iter().any(|&(from, to)| from <= k && k < to);
    let shadowed = s.is(k, "set_config") && !qualified && changed;
    match change {
        Timeout::Set { bounds: true, .. } if !unconditional => None,
        // A transaction end that may not run must not save or restore a bound.
        Timeout::Commit if !unconditional => Some(Timeout::MaybeCommit),
        Timeout::Rollback if !unconditional => Some(Timeout::RollbackToSavepoint),
        Timeout::Set { .. } if shadowed => Some(Timeout::Set {
            bounds: false,
            local: false,
        }),
        other => Some(other),
    }
}

/// Where a change at `k` takes effect.
///
/// PL/pgSQL evaluates the expression of an `EXECUTE` before it runs the SQL.
/// So a change in the expression takes effect at the `EXECUTE` token, before
/// the locks of the SQL.
fn execute_at(s: &Stmts, k: usize) -> usize {
    (s.starts[k]..k)
        .find(|&j| dynamic_execute(s, j))
        .unwrap_or(k)
}

/// Whether the token at `k` is a PL/pgSQL `EXECUTE` of dynamic SQL.
///
/// It may open a statement or follow `FOR ... IN`, `RETURN QUERY` or
/// `OPEN ... FOR`. The `EXECUTE FUNCTION` of a trigger runs no dynamic SQL.
fn dynamic_execute(s: &Stmts, k: usize) -> bool {
    // `EXECUTE` is a non-reserved word, so elsewhere it names a column or a
    // variable, as in `PERFORM execute FROM t` or `execute := 1`.
    let opens = s.starts[k] == k
        || ["in", "query", "for"]
            .iter()
            .any(|w| k > 0 && s.keyword(k - 1, w));
    let assigned = s.is_punct(k + 1, ':') || s.is_punct(k + 1, '=');
    s.keyword(k, "execute")
        && s.toks[k].depth > 0
        && opens
        && !assigned
        && !s.keyword(k + 1, "function")
        && !s.keyword(k + 1, "procedure")
}

/// The verb of a lock that unreadable `EXECUTE` SQL may take.
const UNREADABLE_EXECUTE: &str = "EXECUTE of SQL the lint cannot read";

/// The `)` that closes the `(` at `open`, at the same depth.
fn closing_paren(s: &Stmts, open: usize) -> Option<usize> {
    let depth = s.toks[open].depth;
    let mut parens = 0_usize;
    for j in open..s.toks.len() {
        if s.toks[j].depth < depth {
            return None;
        }
        if s.toks[j].depth > depth {
            continue;
        }
        if s.is_punct(j, '(') {
            parens += 1;
        } else if s.is_punct(j, ')') {
            parens -= 1;
            if parens == 0 {
                return Some(j);
            }
        }
    }
    None
}

/// `CLUSTER [VERBOSE] [table]` at `k`.
fn cluster(s: &Stmts, k: usize) -> Raw {
    let j = k + 1 + usize::from(s.keyword(k + 1, "verbose"));
    // A bare `CLUSTER` rewrites every clustered table.
    let table = s.qualified_name(j).map(|(t, _)| t);
    Raw::lock(k, "CLUSTER", table)
}

/// `LOCK [TABLE] a, b` or `TRUNCATE [TABLE] a, b` at `k`.
fn lock_or_truncate(s: &Stmts, k: usize) -> Vec<Raw> {
    let j = if s.keyword(k + 1, "table") {
        k + 2
    } else {
        k + 1
    };
    let verb = if s.keyword(k, "lock") {
        "LOCK TABLE"
    } else {
        "TRUNCATE"
    };
    let mut raws: Vec<Raw> = s
        .name_list(j)
        .into_iter()
        .map(|t| Raw::lock(k, verb, Some(t)))
        .collect();
    // CASCADE also truncates every table whose key reaches these.
    if (k..s.end(k)).any(|j| s.keyword(j, "cascade")) {
        raws.push(Raw::lock(k, "TRUNCATE ... CASCADE", None));
    }
    raws
}

/// The lock that a foreign key `REFERENCES` at `k` takes on its target.
///
/// A key that may never exist is remembered too, which fails closed when its
/// table is dropped later.
fn references(s: &Stmts, k: usize, history: &mut History) -> Option<Raw> {
    let (target, _) = s.qualified_name(k + 1)?;
    if let Some(owner) = s.statement_table(s.starts[k]) {
        history
            .references
            .entry(base(&owner).to_string())
            .or_default()
            .insert(target.clone());
    }
    Some(Raw::lock(s.starts[k], "REFERENCES", Some(target)))
}

/// Remember `child` as hot when its `parent` is hot.
///
/// The lint learns it even from a branch that may not run. A wrong guess only
/// makes a later lock stricter. The history only grows, so a detach or a drop
/// keeps the name hot.
fn learn_partition(history: &mut History, parent: Option<&str>, child: Option<&str>) {
    if let (Some(parent), Some(child)) = (parent, child)
        && history.is_hot(parent)
    {
        history.partitions.insert(base(child).to_string());
    }
}

/// The `ALTER` forms that lock a table.
fn alter(s: &Stmts, k: usize, history: &History, raws: &mut Vec<Raw>) {
    match s.word(k + 1).filter(|_| !s.toks[k + 1].quoted) {
        Some("table") => {
            // `ALTER TABLE ALL IN TABLESPACE` moves every table there.
            if s.keyword(k + 2, "all") && s.keyword(k + 3, "in") {
                raws.push(Raw::lock(k, "ALTER TABLE ALL IN TABLESPACE", None));
                return;
            }
            let Some(table) = s.statement_table(k) else {
                return;
            };
            // A unique, primary-key or exclusion constraint builds its index
            // under the ALTER TABLE lock. USING INDEX adopts an index instead,
            // but only for the action that names it. `USING INDEX TABLESPACE`
            // only places the new index, so it still builds one.
            let builds_index = s.actions(k).into_iter().any(|(from, to)| {
                let adopts = (from..to).any(|j| {
                    s.keyword(j, "using")
                        && s.keyword(j + 1, "index")
                        && !s.keyword(j + 2, "tablespace")
                });
                !adopts
                    && (from..to).any(|j| {
                        s.keyword(j, "unique")
                            || s.keyword(j, "exclude")
                            || (s.keyword(j, "primary") && s.keyword(j + 1, "key"))
                    })
            });
            if builds_index {
                raws.push(Raw {
                    at: k,
                    verb: "ALTER TABLE ADD UNIQUE, PRIMARY KEY or EXCLUDE",
                    index: None,
                    table: Some(table.clone()),
                    kind: Kind::Index { concurrent: false },
                });
            }
            if drops_dependents(s, k) {
                raws.extend(
                    referenced(history, &table)
                        .map(|t| Raw::lock(k, "ALTER TABLE DROP CONSTRAINT", Some(t))),
                );
            }
            // ATTACH and DETACH PARTITION also lock the partition they name.
            // INHERIT and NO INHERIT also lock the parent.
            for j in k..s.end(k) {
                if (s.keyword(j, "attach") || s.keyword(j, "detach"))
                    && s.keyword(j + 1, "partition")
                    && let Some((partition, _)) = s.qualified_name(j + 2)
                {
                    raws.push(Raw::lock(k, "ALTER TABLE ... PARTITION", Some(partition)));
                }
                if s.keyword(j, "inherit")
                    && let Some((parent, _)) = s.qualified_name(j + 1)
                {
                    raws.push(Raw::lock(k, "ALTER TABLE ... INHERIT", Some(parent)));
                }
            }
            raws.push(Raw::lock(k, "ALTER TABLE", Some(table)));
        }
        Some("trigger") => raws.push(Raw::lock(k, "ALTER TRIGGER", s.name_after(k + 2, "on"))),
        Some("policy") => raws.push(Raw::lock(k, "ALTER POLICY", s.name_after(k + 2, "on"))),
        Some("rule") => raws.push(Raw::lock(k, "ALTER RULE", s.name_after(k + 2, "on"))),
        Some("index") => {
            if let Some((index, _)) = s.qualified_name(s.skip_if_exists(k + 2)) {
                raws.push(Raw {
                    at: k,
                    verb: "ALTER INDEX",
                    index: Some(index),
                    table: None,
                    kind: Kind::Lock,
                });
            }
        }
        _ => {}
    }
}

/// The tables that `table`'s foreign keys reference.
fn referenced<'h>(history: &'h History, table: &str) -> impl Iterator<Item = String> + 'h {
    history
        .references
        .get(base(table))
        .into_iter()
        .flatten()
        .cloned()
}

/// `VACUUM FULL [name, ...]` or `VACUUM (FULL, ...) [name, ...]`.
///
/// A bare `VACUUM FULL` rewrites every table, so its table is unknown.
fn vacuum_full(s: &Stmts, k: usize) -> Vec<Raw> {
    let mut j = k + 1;
    let mut full = false;
    if s.is_punct(j, '(') {
        while j < s.toks.len() && !s.is_punct(j, ')') {
            // `FULL` may carry a boolean, as in `FULL false`.
            if s.keyword(j, "full") {
                let value = s.word(j + 1).or_else(|| s.string(j + 1));
                full = !value.is_some_and(pg_false);
            }
            j += 1;
        }
        j += 1;
    } else {
        while let Some(option @ ("full" | "freeze" | "verbose" | "analyze")) =
            s.word(j).filter(|_| !s.toks[j].quoted)
        {
            full |= option == "full";
            j += 1;
        }
    }
    if !full {
        return Vec::new();
    }
    // Each table may carry a column list: `VACUUM FULL t (a, b), u`.
    let mut names = Vec::new();
    loop {
        if s.keyword(j, "only") {
            j += 1;
        }
        let Some((name, mut next)) = s.qualified_name(j) else {
            break;
        };
        names.push(name);
        next += usize::from(s.is_punct(next, '*'));
        if s.is_punct(next, '(') {
            while next < s.toks.len() && !s.is_punct(next, ')') {
                next += 1;
            }
            next += 1;
        }
        if !s.is_punct(next, ',') {
            break;
        }
        j = next + 1;
    }
    if names.is_empty() {
        return vec![Raw::lock(k, "VACUUM FULL", None)];
    }
    names
        .into_iter()
        .map(|t| Raw::lock(k, "VACUUM FULL", Some(t)))
        .collect()
}

/// `REINDEX [(options)] {INDEX | TABLE | SCHEMA | DATABASE | SYSTEM} [CONCURRENTLY] name`.
///
/// The schema, database and system forms reach every table, so their table
/// is unknown.
fn reindex(s: &Stmts, k: usize) -> Option<Raw> {
    let mut j = k + 1;
    let mut concurrent = false;
    if s.is_punct(j, '(') {
        while j < s.toks.len() && !s.is_punct(j, ')') {
            // `CONCURRENTLY` alone, or with a true value, turns it on. Any
            // other value turns it off or is unknown, which fails closed.
            if s.keyword(j, "concurrently") {
                let value = s.word(j + 1).or_else(|| s.string(j + 1));
                let on =
                    s.is_punct(j + 1, ',') || s.is_punct(j + 1, ')') || value.is_some_and(pg_true);
                concurrent = on;
            }
            j += 1;
        }
        j += 1;
    }
    let scope = s.word(j)?;
    if !["index", "table", "schema", "database", "system"].contains(&scope) {
        return None;
    }
    j += 1;
    if s.keyword(j, "concurrently") {
        concurrent = true;
        j += 1;
    }
    let name = s.qualified_name(j).map(|(name, _)| name);
    let (index, table) = match scope {
        "index" => (name, None),
        "table" => (None, name),
        _ => (None, None),
    };
    Some(Raw {
        at: k,
        verb: "REINDEX",
        index,
        table,
        kind: Kind::Index { concurrent },
    })
}

/// Whether the value at `k` is a non-zero timeout.
///
/// `0` turns the timeout off. `DEFAULT` restores the server default, which is
/// usually `0`, so it does not count either. Neither does a value under 1 ms.
fn bounds_wait(s: &Stmts, k: usize) -> bool {
    let value = match s.toks.get(k).map(|t| &t.tok) {
        Some(Tok::Str(v) | Tok::Word(v)) => v.trim(),
        _ => return false,
    };
    // Postgres stores the value as whole milliseconds, so a value under 1 ms
    // can round to 0. An unknown unit does not count either.
    let split = value
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(value.len());
    let Ok(number) = value[..split].parse::<f64>() else {
        return false;
    };
    let ms_per_unit = match value[split..].trim() {
        "us" => 0.001,
        "" | "ms" => 1.0,
        "s" => 1_000.0,
        "min" => 60_000.0,
        "h" => 3_600_000.0,
        "d" => 86_400_000.0,
        _ => return false,
    };
    number * ms_per_unit >= 1.0
}

/// Read Diesel's `run_in_transaction` out of a `metadata.toml`.
///
/// Diesel and `autumn_harvest::migrate` parse the file with the `toml` crate.
/// This lint uses the same crate, so all three read the file the same way.
/// `migrate` needs the `db` feature, which this lint runs without.
fn run_in_transaction(metadata: &str) -> bool {
    let table: toml::Table =
        toml::from_str(metadata).unwrap_or_else(|e| panic!("metadata.toml is not TOML: {e}"));
    table.get("run_in_transaction").is_none_or(|value| {
        value
            .as_bool()
            .unwrap_or_else(|| panic!("run_in_transaction must be a boolean, found {value}"))
    })
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
        let key = format!("{}/{}", self.tree, self.name);
        self.version() > LOCK_SAFETY_CUTOFF || !legacy_migrations().contains(key.as_str())
    }
}

/// A 64-bit FNV-1a hash of `entries`, in sorted order, one per line.
fn digest(entries: &BTreeSet<&str>) -> u64 {
    entries
        .iter()
        .flat_map(|entry| entry.bytes().chain(*b"\n"))
        .fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
        })
}

/// The entries of `LEGACY_MIGRATIONS`, without comments and blank lines.
fn legacy_migrations() -> BTreeSet<&'static str> {
    LEGACY_MIGRATIONS
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect()
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

/// Lint every migration on disk, each against the history before it.
///
/// Migrations run in the order their database applies them. On a dedicated
/// Harvest database, every core migration runs before any plugin one. The
/// app tree targets its own database, so it keeps its own history.
fn lint_all(migrations: &[OnDisk]) -> Vec<Vec<Finding>> {
    let rank = |m: &OnDisk| match m.tree {
        "autumn-harvest/migrations" => 0,
        APP_TREE => 2,
        _ => 1,
    };
    let mut order: Vec<usize> = (0..migrations.len()).collect();
    order.sort_by(|&a, &b| {
        let (a, b) = (&migrations[a], &migrations[b]);
        (rank(a), &a.name).cmp(&(rank(b), &b.name))
    });
    let mut harvest = History::default();
    let mut app = History::default();
    let mut out = vec![Vec::new(); migrations.len()];
    for i in order {
        let m = &migrations[i];
        let history = if m.tree == APP_TREE {
            &mut app
        } else {
            &mut harvest
        };
        out[i] = lint(&m.sql, m.run_in_transaction, history);
        analyse(&m.sql, history);
    }
    out
}

fn grandfathered(name: &str, rule: Rule) -> bool {
    GRANDFATHERED
        .iter()
        .any(|(entry, entry_rule, _)| *entry == name && *entry_rule == rule)
}

/// Lint a synthetic migration after the synthetic migrations in `history`.
fn lint_with_history(history: &[&str], sql: &str, run_in_transaction: bool) -> Vec<Finding> {
    lint(
        sql,
        run_in_transaction,
        &History::of(history.iter().copied()),
    )
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
    let concurrent = "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_x ON harvest_task_queue (id);";
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
    let sql = "CREATE TABLE harvest_signals (id BIGINT);\n\
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
    let sql = "CREATE INDEX CONCURRENTLY idx_x ON harvest_task_queue (id);";
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

// ── Edge cases ───────────────────────────────────────────────────────────────

#[test]
fn a_create_table_exempts_only_the_statements_after_it() {
    // A table swap locks the live table before the new one exists.
    for sql in [
        "ALTER TABLE harvest_events RENAME TO harvest_events_old;\n\
         CREATE TABLE harvest_events (id INT);",
        "DROP TABLE harvest_events;\nCREATE TABLE harvest_events (id INT);",
    ] {
        let findings = lint_with_history(&[], sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}: {findings:?}");
    }
}

#[test]
fn create_table_if_not_exists_does_not_make_a_table_new() {
    // The statement does nothing when the table already exists.
    let sql = "CREATE TABLE IF NOT EXISTS harvest_events (id INT);\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn a_dollar_quoted_string_cannot_hide_the_sql_after_it() {
    for body in ["it's a table", "a -- b", "see /* here"] {
        let sql = format!(
            "COMMENT ON TABLE harvest_schedules IS $${body}$$;\n\
             ALTER TABLE harvest_events ADD COLUMN x INT;"
        );
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}: {findings:?}");
        assert_eq!(findings[0].line, 2, "{findings:?}");
    }
}

#[test]
fn a_lock_timeout_that_is_turned_off_again_does_not_count() {
    for off in [
        "SET LOCAL lock_timeout = 0;",
        "SET lock_timeout TO DEFAULT;",
        "RESET lock_timeout;",
        "RESET ALL;",
        "SELECT set_config('lock_timeout', '0', true);",
    ] {
        let sql = format!(
            "SET LOCAL lock_timeout = '5s';\n{off}\nALTER TABLE harvest_events ADD COLUMN x INT;"
        );
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{off}: {findings:?}");
    }
    // Every lock needs the bound, not only the first one.
    let sql = "SET LOCAL lock_timeout = '5s';\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;\n\
               RESET lock_timeout;\n\
               ALTER TABLE harvest_timers ADD COLUMN y INT;\n";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
    assert_eq!(findings[0].line, 4, "{findings:?}");
}

#[test]
fn a_foreign_key_finding_names_the_line_where_its_statement_starts() {
    let sql = "ALTER TABLE harvest_schedules\n    ADD CONSTRAINT fk FOREIGN KEY (e)\n    \
               REFERENCES harvest_events (id);";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
    assert_eq!(findings[0].line, 1, "{findings:?}");

    let annotated = format!("-- lock-safety: allow lock-timeout #1810 a reviewed reason\n{sql}");
    assert_eq!(lint_with_history(&[], &annotated, true), []);
}

#[test]
fn table_rewrites_and_wide_reindexes_are_flagged() {
    for (sql, in_transaction) in [
        ("CLUSTER harvest_events USING idx_x;", true),
        ("CLUSTER;", true),
        ("VACUUM FULL harvest_events;", false),
        ("VACUUM (FULL, ANALYZE) harvest_events;", false),
    ] {
        let findings = lint_with_history(&[], sql, in_transaction);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}: {findings:?}");
    }
    assert_eq!(
        lint_with_history(&[], "VACUUM ANALYZE harvest_events;", false),
        []
    );
    for scope in ["SCHEMA public", "DATABASE app", "SYSTEM app"] {
        let sql = format!("SET LOCAL lock_timeout = '5s';\nREINDEX {scope};");
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(
            rules(&findings),
            [Rule::BlockingIndex],
            "{sql}: {findings:?}"
        );
    }
}

#[test]
fn the_reindex_concurrently_option_form_is_concurrent() {
    let sql = "REINDEX (CONCURRENTLY, VERBOSE) TABLE harvest_events;";
    assert_eq!(lint_with_history(&[], sql, false), []);
    assert_eq!(
        rules(&lint_with_history(&[], sql, true)),
        [Rule::ConcurrentlyInTransaction]
    );
}

#[test]
fn only_a_set_statement_sets_the_session_timeout() {
    for set in [
        "ALTER DATABASE app SET lock_timeout = '5s';",
        "ALTER ROLE app SET lock_timeout = '5s';",
    ] {
        let sql = format!("{set}\nALTER TABLE harvest_events ADD COLUMN x INT;");
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{set}: {findings:?}");
    }
    let in_block = "DO $$\nBEGIN\n    SET LOCAL lock_timeout = '5s';\n    \
                    ALTER TABLE harvest_events ADD COLUMN x INT;\nEND $$;";
    assert_eq!(lint_with_history(&[], in_block, true), []);
}

#[test]
fn drop_index_resolves_against_earlier_definitions_only() {
    let history = ["CREATE INDEX idx_a ON harvest_events (id);"];
    let sql = "SET LOCAL lock_timeout = '5s';\nDROP INDEX idx_a;\n\
               CREATE INDEX idx_a ON harvest_schedules (id);";
    let findings = lint_with_history(&history, sql, true);
    assert_eq!(rules(&findings), [Rule::BlockingIndex], "{findings:?}");
    assert_eq!(findings[0].line, 2);
}

#[test]
fn rule_policy_trigger_and_index_ddl_needs_a_lock_timeout() {
    let history = ["CREATE INDEX idx_hot ON harvest_events (id);"];
    for sql in [
        "CREATE RULE r AS ON INSERT TO harvest_events DO INSTEAD NOTHING;",
        "CREATE POLICY p ON harvest_events USING (true);",
        "ALTER POLICY p ON harvest_events USING (true);",
        "DROP POLICY IF EXISTS p ON harvest_events;",
        "ALTER TRIGGER t ON harvest_events RENAME TO u;",
        "ALTER INDEX idx_hot SET TABLESPACE fast;",
    ] {
        let findings = lint_with_history(&history, sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}: {findings:?}");
    }
}

#[test]
fn concurrently_inside_a_do_block_is_flagged() {
    // Postgres rejects CONCURRENTLY inside a function or a DO block.
    let sql =
        "DO $$\nBEGIN\n    CREATE INDEX CONCURRENTLY idx_x ON harvest_task_queue (id);\nEND $$;";
    let findings = lint_with_history(&[], sql, false);
    assert_eq!(
        rules(&findings),
        [Rule::ConcurrentlyInTransaction],
        "{findings:?}"
    );
    assert_eq!(findings[0].line, 3);
}

#[test]
fn a_transaction_local_timeout_counts_in_a_non_transactional_batch() {
    // Diesel sends the file as one batch, and Postgres runs a batch as one
    // implicit transaction. `SET LOCAL` therefore holds until the batch ends.
    let sql = "SET LOCAL lock_timeout = '5s';\nALTER TABLE harvest_events ADD COLUMN x INT;";
    assert_eq!(lint_with_history(&[], sql, false), []);
}

#[test]
fn concurrently_must_be_alone_in_a_non_transactional_file() {
    for extra in [
        "SET lock_timeout = '5s';",
        "CREATE INDEX CONCURRENTLY idx_y ON harvest_task_queue (y);",
    ] {
        let sql = format!("{extra}\nCREATE INDEX CONCURRENTLY idx_x ON harvest_task_queue (x);");
        let findings = lint_with_history(&[], &sql, false);
        assert!(
            rules(&findings).contains(&Rule::ConcurrentlyInTransaction),
            "{sql}: {findings:?}"
        );
    }
}

#[test]
fn the_claim_path_tables_are_hot() {
    // Every claim reads these, so a waiting ACCESS EXCLUSIVE stalls claims.
    for table in [
        "harvest_activity_pauses",
        "harvest_queue_pauses",
        "harvest_rate_limit_buckets",
        "harvest_shard_generation",
        "harvest_workers",
    ] {
        let sql = format!("ALTER TABLE {table} ADD COLUMN x INT;");
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(
            rules(&findings),
            [Rule::LockTimeout],
            "{table}: {findings:?}"
        );
    }
}

#[test]
fn dropping_a_foreign_key_locks_the_table_it_references() {
    // Postgres drops the foreign-key triggers on the referenced table too.
    let history =
        ["CREATE TABLE harvest_child (exec_id UUID REFERENCES harvest_workflow_executions (id));"];
    for sql in [
        "DROP TABLE harvest_child;",
        "ALTER TABLE harvest_child DROP CONSTRAINT harvest_child_exec_id_fkey;",
        // A dropped column takes its foreign key along. A type change rebuilds
        // the key.
        "ALTER TABLE harvest_child DROP COLUMN exec_id;",
        "ALTER TABLE harvest_child DROP exec_id;",
        "ALTER TABLE harvest_child ALTER COLUMN exec_id TYPE TEXT;",
        "ALTER TABLE harvest_child ALTER exec_id SET DATA TYPE TEXT;",
    ] {
        let findings = lint_with_history(&history, sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}: {findings:?}");
        assert!(
            findings[0].detail.contains("harvest_workflow_executions"),
            "{findings:?}"
        );
    }
    // A table with no foreign key to a hot table locks nothing hot.
    assert_eq!(
        lint_with_history(&history, "DROP TABLE harvest_schedules;", true),
        []
    );
    // A column default or nullability change leaves the key alone.
    let sql = "ALTER TABLE harvest_child ALTER COLUMN exec_id DROP NOT NULL;";
    assert_eq!(lint_with_history(&history, sql, true), []);
}

#[test]
fn a_unique_constraint_on_a_hot_table_is_a_blocking_index_build() {
    let set = "SET LOCAL lock_timeout = '5s';\n";
    for add in [
        "ALTER TABLE harvest_events ADD CONSTRAINT u UNIQUE (id);",
        "ALTER TABLE harvest_events ADD PRIMARY KEY (id);",
        "ALTER TABLE harvest_events ADD COLUMN k INT UNIQUE;",
    ] {
        let findings = lint_with_history(&[], &format!("{set}{add}"), true);
        assert_eq!(
            rules(&findings),
            [Rule::BlockingIndex],
            "{add}: {findings:?}"
        );
    }
    let adopt = "ALTER TABLE harvest_events ADD CONSTRAINT u UNIQUE USING INDEX idx_u;";
    assert_eq!(lint_with_history(&[], &format!("{set}{adopt}"), true), []);
}

#[test]
fn a_partition_of_a_hot_table_locks_the_parent() {
    let sql = "CREATE TABLE harvest_events_p1 PARTITION OF harvest_events \
               FOR VALUES FROM (1) TO (2);";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn a_timeout_set_inside_a_function_body_does_not_count() {
    // A function body runs only when someone calls the function.
    let sql = "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$\n\
               BEGIN\n    PERFORM set_config('lock_timeout', '5s', true);\nEND $$;\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;\n";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");

    let in_do = "DO $$\nBEGIN\n    PERFORM set_config('lock_timeout', '5s', true);\nEND $$;\n\
                 ALTER TABLE harvest_events ADD COLUMN x INT;\n";
    assert_eq!(lint_with_history(&[], in_do, true), []);
}

#[test]
fn every_unbounded_lock_gets_its_own_finding() {
    // An annotation on the first lock must not cover a later one.
    let sql = "-- lock-safety: allow lock-timeout #1810 a reviewed reason\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;\n\
               ALTER TABLE harvest_timers ADD COLUMN y INT;\n";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
    assert_eq!(findings[0].line, 3, "{findings:?}");
}

#[test]
fn using_index_exempts_only_its_own_alter_action() {
    let sql = "SET LOCAL lock_timeout = '5s';\n\
               ALTER TABLE harvest_events ADD CONSTRAINT u UNIQUE (a), \
               ADD CONSTRAINT p PRIMARY KEY USING INDEX idx_p;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::BlockingIndex], "{findings:?}");
}

#[test]
fn a_create_table_in_a_function_body_does_not_make_a_table_new() {
    let sql = "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$\n\
               BEGIN\n    CREATE TABLE harvest_events (id INT);\nEND $$;\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;\n";
    let findings = lint_with_history(&[], sql, true);
    assert!(
        findings
            .iter()
            .any(|f| f.rule == Rule::LockTimeout && f.line == 5),
        "{findings:?}"
    );
}

#[test]
fn drop_rule_locks_its_table() {
    let findings = lint_with_history(&[], "DROP RULE IF EXISTS r ON harvest_events;", true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn a_timeout_set_on_a_conditional_path_does_not_count() {
    let lock = "\nALTER TABLE harvest_events ADD COLUMN x INT;\n";
    for body in [
        "IF false THEN\n    PERFORM set_config('lock_timeout', '5s', true);\nEND IF;",
        "FOR i IN 1..0 LOOP\n    PERFORM set_config('lock_timeout', '5s', true);\nEND LOOP;",
        "NULL;\nEXCEPTION WHEN others THEN\n    PERFORM set_config('lock_timeout', '5s', true);",
    ] {
        let sql = format!("DO $$\nBEGIN\n{body}\nEND $$;{lock}");
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(
            rules(&findings),
            [Rule::LockTimeout],
            "{body}: {findings:?}"
        );
    }
    // A setter after a closed branch runs on every path.
    let after = format!(
        "DO $$\nBEGIN\nIF false THEN\n    NULL;\nEND IF;\n\
         PERFORM set_config('lock_timeout', '5s', true);\nEND $$;{lock}"
    );
    assert_eq!(lint_with_history(&[], &after, true), []);
}

#[test]
fn a_guarded_create_index_does_not_rewrite_index_history() {
    // `IF NOT EXISTS` does nothing when the name exists, so the old table stays.
    let history = ["CREATE INDEX idx_hot ON harvest_events (id);"];
    let sql = "SET LOCAL lock_timeout = '5s';\n\
               CREATE INDEX IF NOT EXISTS idx_hot ON harvest_schedules (id);\n\
               DROP INDEX idx_hot;";
    let findings = lint_with_history(&history, sql, true);
    assert_eq!(rules(&findings), [Rule::BlockingIndex], "{findings:?}");
    assert_eq!(findings[0].line, 3, "{findings:?}");
}

#[test]
fn a_conditional_timeout_clear_ends_the_bound() {
    let sql = "SET LOCAL lock_timeout = '5s';\n\
               DO $$\nBEGIN\nIF random() > 0.5 THEN\n    \
               PERFORM set_config('lock_timeout', '0', true);\nEND IF;\nEND $$;\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;\n";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn a_conditional_create_table_does_not_make_a_table_new() {
    let sql = "DO $$\nBEGIN\nIF random() > 0.5 THEN\n    \
               CREATE TABLE harvest_events (id INT);\nEND IF;\nEND $$;\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;\n";
    let findings = lint_with_history(&[], sql, true);
    assert!(
        findings
            .iter()
            .any(|f| f.rule == Rule::LockTimeout && f.line == 7),
        "{findings:?}"
    );
}

#[test]
fn a_new_table_in_another_schema_does_not_exempt_the_hot_table() {
    let sql = "CREATE TABLE staging.harvest_events (id INT);\n\
               ALTER TABLE public.harvest_events ADD COLUMN x INT;\n";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
    assert_eq!(findings[0].line, 2, "{findings:?}");
}

#[test]
fn a_foreign_key_is_remembered_even_from_a_body_that_may_not_run() {
    // Remembering a key that never ran fails closed: a later drop is flagged.
    let history = ["DO $$\nBEGIN\nIF random() > 0.5 THEN\n    \
                    CREATE TABLE harvest_child (e UUID REFERENCES harvest_events (id));\n\
                    END IF;\nEND $$;"];
    let findings = lint_with_history(&history, "DROP TABLE harvest_child;", true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn a_set_config_in_a_filtered_query_does_not_count() {
    // The function runs only when the query returns a row.
    let lock = "\nALTER TABLE harvest_events ADD COLUMN x INT;";
    for set in [
        "SELECT set_config('lock_timeout', '5s', true) WHERE false;",
        "SELECT set_config('lock_timeout', '5s', true) FROM harvest_schedules;",
    ] {
        let findings = lint_with_history(&[], &format!("{set}{lock}"), true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{set}: {findings:?}");
    }
}

#[test]
fn alter_table_all_in_tablespace_locks_an_unknown_table() {
    let sql = "ALTER TABLE ALL IN TABLESPACE old SET TABLESPACE fast;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn an_index_name_in_another_schema_cannot_hide_a_hot_index() {
    let history = [
        "CREATE INDEX idx_shared ON public.harvest_events (id);",
        "CREATE INDEX idx_shared ON staging.harvest_schedules (id);",
    ];
    let sql = "SET LOCAL lock_timeout = '5s';\nDROP INDEX public.idx_shared;";
    let findings = lint_with_history(&history, sql, true);
    assert_eq!(rules(&findings), [Rule::BlockingIndex], "{findings:?}");
}

#[test]
fn a_transaction_end_clears_a_local_timeout() {
    for end in ["COMMIT", "ROLLBACK", "END"] {
        let sql = format!(
            "BEGIN;\nSET LOCAL lock_timeout = '5s';\n{end};\n\
             ALTER TABLE harvest_events ADD COLUMN x INT;"
        );
        let findings = lint_with_history(&[], &sql, false);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{end}: {findings:?}");
    }
    // A session-level timeout outlives the transaction.
    let session = "BEGIN;\nSET lock_timeout = '5s';\nCOMMIT;\n\
                   ALTER TABLE harvest_events ADD COLUMN x INT;";
    assert_eq!(lint_with_history(&[], session, false), []);
}

#[test]
fn an_annotation_allows_one_statement_only() {
    let sql = "-- lock-safety: allow lock-timeout #1810 a reviewed reason\n\
               ALTER TABLE harvest_events ADD COLUMN x INT; ALTER TABLE harvest_timers ADD COLUMN y INT;\n";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
    // One statement that drops two indexes needs one annotation, not two.
    let history =
        ["CREATE INDEX a ON harvest_events (id);\nCREATE INDEX b ON harvest_events (id);"];
    let one = "SET LOCAL lock_timeout = '5s';\n\
               -- lock-safety: allow blocking-index #1810 a reviewed reason\n\
               DROP INDEX a, b;";
    assert_eq!(lint_with_history(&history, one, true), []);
}

#[test]
fn a_name_list_continues_past_an_inheritance_marker() {
    for sql in [
        "LOCK TABLE harvest_schedules *, harvest_events IN ACCESS EXCLUSIVE MODE;",
        "TRUNCATE harvest_schedules *, harvest_timers;",
    ] {
        let findings = lint_with_history(&[], sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}: {findings:?}");
    }
}

#[test]
fn a_rollback_restores_the_session_timeout_from_before_the_transaction() {
    let sql = "BEGIN;\nSET lock_timeout = '5s';\nROLLBACK;\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;";
    let findings = lint_with_history(&[], sql, false);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
    // The file runs as one implicit transaction, so a `BEGIN` inside it
    // starts nothing new. The rollback also undoes the `SET` before it.
    let before = "SET lock_timeout = '5s';\nBEGIN;\nSET lock_timeout = 0;\nROLLBACK;\n\
                  ALTER TABLE harvest_events ADD COLUMN x INT;";
    let findings = lint_with_history(&[], before, false);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
    // A bound set after a commit survives a later rollback.
    let committed = "SET lock_timeout = '5s';\nCOMMIT;\nBEGIN;\nSET lock_timeout = 0;\n\
                     ROLLBACK;\nALTER TABLE harvest_events ADD COLUMN x INT;";
    assert_eq!(lint_with_history(&[], committed, false), []);
}

#[test]
fn code_after_a_return_does_not_surely_run() {
    for exit in ["IF random() > 0.5 THEN\n    RETURN;\nEND IF;", "RETURN;"] {
        let sql = format!(
            "DO $$\nBEGIN\n{exit}\nPERFORM set_config('lock_timeout', '5s', true);\nEND $$;\n\
             ALTER TABLE harvest_events ADD COLUMN x INT;"
        );
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(
            rules(&findings),
            [Rule::LockTimeout],
            "{exit}: {findings:?}"
        );
    }
}

#[test]
fn a_renamed_table_keeps_its_foreign_keys() {
    let history = [
        "CREATE TABLE harvest_child (e UUID REFERENCES harvest_events (id));",
        "ALTER TABLE harvest_child RENAME TO renamed_child;",
    ];
    for sql in [
        "DROP TABLE renamed_child;",
        "ALTER TABLE renamed_child DROP CONSTRAINT harvest_child_e_fkey;",
    ] {
        let findings = lint_with_history(&history, sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}: {findings:?}");
    }
}

#[test]
fn attach_and_detach_partition_lock_the_partition() {
    for sql in [
        "ALTER TABLE harvest_schedules ATTACH PARTITION harvest_events FOR VALUES FROM (1) TO (2);",
        "ALTER TABLE harvest_schedules DETACH PARTITION harvest_events;",
    ] {
        let findings = lint_with_history(&[], sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}: {findings:?}");
        assert!(
            findings[0].detail.contains("harvest_events"),
            "{findings:?}"
        );
    }
}

#[test]
fn a_rollback_undoes_a_new_table() {
    for undo in ["ROLLBACK;", "ROLLBACK TO SAVEPOINT s;"] {
        let sql = format!(
            "BEGIN;\nSAVEPOINT s;\nCREATE TABLE harvest_events (id INT);\n{undo}\n\
             ALTER TABLE harvest_events ADD COLUMN x INT;"
        );
        let findings = lint_with_history(&[], &sql, false);
        assert_eq!(
            rules(&findings),
            [Rule::LockTimeout],
            "{undo}: {findings:?}"
        );
    }
}

#[test]
fn a_vacuum_list_continues_past_a_column_list() {
    let sql = "VACUUM FULL harvest_schedules (id, name), harvest_events;";
    let findings = lint_with_history(&[], sql, false);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
    assert!(
        findings[0].detail.contains("harvest_events"),
        "{findings:?}"
    );
}

#[test]
fn the_app_database_keeps_its_own_index_history() {
    let app = OnDisk {
        tree: APP_TREE,
        name: "20260101000000_app".to_string(),
        sql: "CREATE INDEX idx_shared ON harvest_schedules (id);".to_string(),
        run_in_transaction: true,
    };
    let core = OnDisk {
        tree: "autumn-harvest/migrations",
        name: "20260102000000_core".to_string(),
        sql: "SET LOCAL lock_timeout = '5s';\nDROP INDEX idx_shared;".to_string(),
        run_in_transaction: true,
    };
    let all = lint_all(&[app, core]);
    // The core database never saw the app index, so its table is unknown.
    assert_eq!(rules(&all[1]), [Rule::BlockingIndex], "{all:?}");
}

#[test]
fn alter_rule_locks_its_table() {
    let findings = lint_with_history(&[], "ALTER RULE r ON harvest_events RENAME TO r2;", true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn a_nested_dollar_string_keeps_the_branch_state() {
    let sql = "DO $$\nBEGIN\nIF random() > 0.5 THEN\n    PERFORM $q$text$q$;\n    \
               PERFORM set_config('lock_timeout', '5s', true);\nEND IF;\nEND $$;\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn a_guarded_create_index_teaches_nothing() {
    // An index of that name may already exist on a hot table.
    let history = ["CREATE INDEX IF NOT EXISTS idx_maybe ON harvest_schedules (id);"];
    let sql = "SET LOCAL lock_timeout = '5s';\nDROP INDEX idx_maybe;";
    let findings = lint_with_history(&history, sql, true);
    assert_eq!(rules(&findings), [Rule::BlockingIndex], "{findings:?}");
}

#[test]
fn vacuum_full_only_names_the_table_after_only() {
    let findings = lint_with_history(&[], "VACUUM FULL ONLY harvest_events;", false);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
    assert!(
        findings[0].detail.contains("harvest_events"),
        "{findings:?}"
    );
}

#[test]
fn an_exception_handler_may_undo_its_whole_block() {
    // The handler rolls back the block, so nothing in it surely happens.
    let create = "DO $$\nBEGIN\n    CREATE TABLE harvest_events (id INT);\n    \
                  PERFORM 1 / 0;\nEXCEPTION WHEN others THEN\n    NULL;\nEND $$;\n\
                  ALTER TABLE harvest_events ADD COLUMN x INT;";
    let findings = lint_with_history(&[], create, true);
    assert!(
        findings
            .iter()
            .any(|f| f.rule == Rule::LockTimeout && f.line == 8),
        "{findings:?}"
    );
    let setter = "DO $$\nBEGIN\n    PERFORM set_config('lock_timeout', '5s', true);\n    \
                  PERFORM 1 / 0;\nEXCEPTION WHEN others THEN\n    NULL;\nEND $$;\n\
                  ALTER TABLE harvest_events ADD COLUMN x INT;";
    let findings = lint_with_history(&[], setter, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn cascade_reaches_unknown_tables() {
    // CASCADE also locks every table whose foreign key reaches the target.
    for sql in [
        "TRUNCATE harvest_schedules CASCADE;",
        "DROP TABLE harvest_schedules CASCADE;",
    ] {
        let findings = lint_with_history(&[], sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}: {findings:?}");
    }
}

#[test]
fn a_new_table_stops_being_new_once_it_is_dropped_or_renamed() {
    for gone in [
        "DROP TABLE harvest_events;",
        "ALTER TABLE harvest_events RENAME TO staged_events;",
    ] {
        let sql = format!(
            "CREATE TABLE harvest_events (id INT);\n{gone}\n\
             ALTER TABLE harvest_events ADD COLUMN x INT;"
        );
        let findings = lint_with_history(&[], &sql, true);
        assert!(
            findings
                .iter()
                .any(|f| f.rule == Rule::LockTimeout && f.line == 3),
            "{gone}: {findings:?}"
        );
    }
}

#[test]
fn code_after_an_exit_does_not_surely_run() {
    for exit in [
        "EXIT blk WHEN random() > 0.5;",
        "CONTINUE WHEN random() > 0.5;",
    ] {
        let sql = format!(
            "DO $$\nBEGIN\n<<blk>>\nBEGIN\n{exit}\n\
             PERFORM set_config('lock_timeout', '5s', true);\nEND;\nEND $$;\n\
             ALTER TABLE harvest_events ADD COLUMN x INT;"
        );
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(
            rules(&findings),
            [Rule::LockTimeout],
            "{exit}: {findings:?}"
        );
    }
}

#[test]
fn a_set_config_with_an_unknown_scope_counts_as_local() {
    let sql = "BEGIN;\nSELECT set_config('lock_timeout', '5s', NOT false);\nCOMMIT;\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;";
    let findings = lint_with_history(&[], sql, false);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
    // A literal false is a session-level setting, which outlives the commit.
    let session = "BEGIN;\nSELECT set_config('lock_timeout', '5s', false);\nCOMMIT;\n\
                   ALTER TABLE harvest_events ADD COLUMN x INT;";
    assert_eq!(lint_with_history(&[], session, false), []);
}

#[test]
fn reindex_concurrently_false_is_a_plain_reindex() {
    for option in ["CONCURRENTLY false", "CONCURRENTLY off", "CONCURRENTLY 0"] {
        let sql =
            format!("SET LOCAL lock_timeout = '5s';\nREINDEX ({option}) TABLE harvest_events;");
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(
            rules(&findings),
            [Rule::BlockingIndex],
            "{option}: {findings:?}"
        );
    }
    let on = "REINDEX (CONCURRENTLY true) TABLE harvest_events;";
    assert_eq!(lint_with_history(&[], on, false), []);
}

#[test]
fn rollback_to_a_savepoint_ends_the_bound() {
    let sql = "SET lock_timeout = '5s';\nBEGIN;\nSET lock_timeout = 0;\nSAVEPOINT s;\n\
               SET lock_timeout = '5s';\nROLLBACK TO SAVEPOINT s;\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;";
    let findings = lint_with_history(&[], sql, false);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn inherit_and_no_inherit_lock_the_parent() {
    for sql in [
        "ALTER TABLE harvest_schedules INHERIT harvest_events;",
        "ALTER TABLE harvest_schedules NO INHERIT harvest_events;",
    ] {
        let findings = lint_with_history(&[], sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}: {findings:?}");
        assert!(
            findings[0].detail.contains("harvest_events"),
            "{findings:?}"
        );
    }
}

#[test]
fn a_timeout_that_rounds_to_zero_does_not_count() {
    for value in ["'0.1ms'", "'0.0001s'", "'5 parsecs'"] {
        let sql = format!(
            "SET LOCAL lock_timeout = {value};\nALTER TABLE harvest_events ADD COLUMN x INT;"
        );
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(
            rules(&findings),
            [Rule::LockTimeout],
            "{value}: {findings:?}"
        );
    }
    for value in ["'5s'", "'5 s'", "'1min'", "'250ms'", "5000", "'1h'"] {
        let sql = format!(
            "SET LOCAL lock_timeout = {value};\nALTER TABLE harvest_events ADD COLUMN x INT;"
        );
        assert_eq!(lint_with_history(&[], &sql, true), [], "{value}");
    }
}

#[test]
fn a_renamed_table_keeps_its_indexes() {
    let history = [
        "CREATE TABLE replacement (id INT);\nCREATE INDEX idx_replacement ON replacement (id);",
        "ALTER TABLE replacement RENAME TO harvest_events;",
    ];
    let sql = "SET LOCAL lock_timeout = '5s';\nDROP INDEX idx_replacement;";
    let findings = lint_with_history(&history, sql, true);
    assert_eq!(rules(&findings), [Rule::BlockingIndex], "{findings:?}");
}

#[test]
fn if_not_and_if_exists_branches_are_conditional() {
    for cond in [
        "IF NOT ready THEN",
        "IF EXISTS (SELECT 1 FROM harvest_schedules) THEN",
    ] {
        let sql = format!(
            "DO $$\nDECLARE\n    ready boolean := true;\nBEGIN\n{cond}\n    \
             PERFORM set_config('lock_timeout', '5s', true);\nEND IF;\nEND $$;\n\
             ALTER TABLE harvest_events ADD COLUMN x INT;"
        );
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(
            rules(&findings),
            [Rule::LockTimeout],
            "{cond}: {findings:?}"
        );
    }
}

#[test]
fn core_migrations_never_learn_from_plugin_migrations() {
    // Core runs first on a dedicated database, so an older plugin index has
    // not run yet when a core migration drops a same-named index.
    let plugin = OnDisk {
        tree: "autumn-harvest-plugin/migrations/harvest",
        name: "20260101000000_plugin".to_string(),
        sql: "CREATE INDEX idx_shared ON harvest_schedules (id);".to_string(),
        run_in_transaction: true,
    };
    let core = OnDisk {
        tree: "autumn-harvest/migrations",
        name: "20260102000000_core".to_string(),
        sql: "SET LOCAL lock_timeout = '5s';\nDROP INDEX idx_shared;".to_string(),
        run_in_transaction: true,
    };
    let all = lint_all(&[plugin, core]);
    assert_eq!(rules(&all[1]), [Rule::BlockingIndex], "{all:?}");
}

#[test]
fn a_case_expression_does_not_split_a_statement() {
    let sql = "SET LOCAL lock_timeout = '5s';\n\
               ALTER TABLE harvest_events ADD CHECK (CASE WHEN x THEN true ELSE false END), \
               ADD UNIQUE (event_id);";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::BlockingIndex], "{findings:?}");
}

#[test]
fn a_guarded_build_on_a_hot_table_still_teaches_the_hot_table() {
    let history = [
        "CREATE INDEX idx_shared ON harvest_schedules (id);\nDROP INDEX idx_shared;",
        "SET LOCAL lock_timeout = '5s';\n\
         -- lock-safety: allow blocking-index #1810 a reviewed reason\n\
         CREATE INDEX IF NOT EXISTS idx_shared ON harvest_events (id);",
    ];
    let sql = "SET LOCAL lock_timeout = '5s';\nDROP INDEX idx_shared;";
    let findings = lint_with_history(&history, sql, true);
    assert_eq!(rules(&findings), [Rule::BlockingIndex], "{findings:?}");
}

#[test]
fn a_key_under_a_toml_table_is_not_top_level() {
    assert!(run_in_transaction(
        "[section]\nrun_in_transaction = false\n"
    ));
    assert!(!run_in_transaction(
        "run_in_transaction = false\n[section]\nother = 1\n"
    ));
}

#[test]
fn a_repeated_begin_keeps_the_first_snapshot() {
    let sql = "BEGIN;\nSET lock_timeout = '5s';\nBEGIN;\nSET lock_timeout = '0';\nROLLBACK;\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;";
    let findings = lint_with_history(&[], sql, false);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn renaming_a_referenced_table_moves_the_keys_that_point_at_it() {
    let history = [
        "CREATE TABLE replacement (id INT PRIMARY KEY);\n\
         CREATE TABLE child (r INT REFERENCES replacement (id));",
        "ALTER TABLE harvest_events RENAME TO old_events;\n\
         ALTER TABLE replacement RENAME TO harvest_events;",
    ];
    let findings = lint_with_history(&history, "DROP TABLE child;", true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
    assert!(
        findings[0].detail.contains("harvest_events"),
        "{findings:?}"
    );
}

#[test]
fn nulls_not_distinct_after_the_columns_is_still_a_plain_build() {
    let sql = "SET LOCAL lock_timeout = '5s';\n\
               CREATE UNIQUE INDEX idx_x ON harvest_events (id) NULLS NOT DISTINCT;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::BlockingIndex], "{findings:?}");
}

#[test]
fn concurrently_cannot_reach_a_partitioned_parent() {
    // On the partitioned layout, `harvest_events` is a partitioned parent.
    // Postgres runs neither form concurrently on a partitioned parent.
    let history = ["CREATE INDEX idx_e ON harvest_events (id);"];
    for sql in [
        "CREATE INDEX CONCURRENTLY idx_x ON harvest_events (id);",
        "DROP INDEX CONCURRENTLY idx_e;",
    ] {
        let findings = lint_with_history(&history, sql, false);
        assert_eq!(
            rules(&findings),
            [Rule::BlockingIndex],
            "{sql}: {findings:?}"
        );
        assert!(findings[0].detail.contains("partitioned"), "{findings:?}");
    }
    let other = "CREATE INDEX CONCURRENTLY idx_x ON harvest_task_queue (id);";
    assert_eq!(lint_with_history(&[], other, false), []);
}

#[test]
fn a_sure_drop_forgets_the_index() {
    let history = [
        "CREATE INDEX idx_shared ON harvest_schedules (id);",
        "DROP INDEX idx_shared;",
    ];
    let sql = "SET LOCAL lock_timeout = '5s';\nDROP INDEX idx_shared;";
    let findings = lint_with_history(&history, sql, true);
    assert_eq!(rules(&findings), [Rule::BlockingIndex], "{findings:?}");
}

#[test]
fn a_quoted_metadata_key_is_read() {
    assert!(!run_in_transaction("\"run_in_transaction\" = false\n"));
    assert!(!run_in_transaction("'run_in_transaction' = false\n"));
}

#[test]
fn a_new_transaction_starts_after_a_commit_or_rollback() {
    // The batch opens an implicit block at the next statement. A later
    // `BEGIN` takes over that block, so its snapshot predates the `SET`.
    for end in [
        "COMMIT",
        "ROLLBACK",
        "END",
        "COMMIT AND CHAIN",
        "ROLLBACK AND CHAIN",
    ] {
        let sql = format!(
            "{end};\nSET lock_timeout = '5s';\nBEGIN;\nSET lock_timeout = '0';\nROLLBACK;\n\
             ALTER TABLE harvest_events ADD COLUMN x INT;"
        );
        let findings = lint_with_history(&[], &sql, false);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{end}: {findings:?}");
    }
}

#[test]
fn a_partition_of_a_hot_table_is_hot() {
    let sql = "CREATE INDEX idx_x ON events_p (id);";
    let both = [Rule::LockTimeout, Rule::BlockingIndex];
    for history in [
        vec!["CREATE TABLE events_p PARTITION OF harvest_events FOR VALUES FROM (1) TO (2);"],
        vec![
            "CREATE TABLE events_p (LIKE harvest_events);",
            "SET LOCAL lock_timeout = '5s';\n\
             ALTER TABLE harvest_events ATTACH PARTITION events_p FOR VALUES FROM (1) TO (2);",
        ],
        // A partition of a partition is hot too.
        vec![
            "CREATE TABLE events_q PARTITION OF harvest_events FOR VALUES FROM (1) TO (2) \
             PARTITION BY RANGE (id);",
            "CREATE TABLE events_p PARTITION OF events_q FOR VALUES FROM (1) TO (2);",
        ],
        // A rename keeps the partition hot.
        vec![
            "CREATE TABLE events_q PARTITION OF harvest_events FOR VALUES FROM (1) TO (2);",
            "ALTER TABLE events_q RENAME TO events_p;",
        ],
    ] {
        let mut found = rules(&lint_with_history(&history, sql, true));
        found.sort();
        let mut want = both.to_vec();
        want.sort();
        assert_eq!(found, want, "{history:?}");
    }
    let cold = ["CREATE TABLE sched_p PARTITION OF harvest_schedules FOR VALUES FROM (1) TO (2);"];
    let findings = lint_with_history(&cold, "CREATE INDEX idx_x ON sched_p (id);", true);
    assert_eq!(findings, []);
}

#[test]
fn a_single_quoted_do_body_is_scanned_as_code() {
    for sql in [
        "DO 'BEGIN ALTER TABLE harvest_events ADD COLUMN note TEXT; END';",
        "DO LANGUAGE plpgsql 'BEGIN ALTER TABLE harvest_events ADD COLUMN note TEXT; END';",
        "DO E'BEGIN\\nALTER TABLE harvest_events ADD COLUMN note TEXT;\\nEND';",
        "DO U&'BEGIN ALTER TABLE harvest_events ADD COLUMN note TEXT; END';",
        // A doubled quote inside the body is one quote.
        "DO 'BEGIN RAISE NOTICE ''x''; ALTER TABLE harvest_events ADD COLUMN note TEXT; END';",
    ] {
        let findings = lint_with_history(&[], sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}: {findings:?}");
    }
    // A string that is not a `DO` body stays a string.
    let sql = "SELECT 'ALTER TABLE harvest_events ADD COLUMN note TEXT';";
    assert_eq!(lint_with_history(&[], sql, true), []);
}

#[test]
fn a_runtime_partition_of_harvest_events_is_hot() {
    // The partition manager creates these at run time, outside any migration.
    for table in [
        "harvest_events_p_default",
        "harvest_events_p_20260901000000",
        "harvest_events_legacy",
    ] {
        let sql = format!("CREATE INDEX idx_x ON {table} (id);");
        let mut found = rules(&lint_with_history(&[], &sql, true));
        found.sort();
        assert_eq!(found, [Rule::LockTimeout, Rule::BlockingIndex], "{table}");
    }
}

#[test]
fn a_search_path_change_ends_an_unqualified_exemption() {
    // After the change, the same unqualified name can mean the hot table.
    for change in [
        "SET search_path = public;",
        "SET LOCAL search_path TO public;",
        "RESET search_path;",
        "RESET ALL;",
        "SELECT set_config('search_path', 'public', false);",
        // A computed name may be `search_path`.
        "SELECT set_config('search_' || 'path', 'public', false);",
        // `SET SCHEMA` is an alias of `SET search_path`.
        "SET SCHEMA 'public';",
    ] {
        let sql = format!(
            "SET search_path = scratch;\nCREATE TABLE harvest_events (id INT);\n{change}\n\
             ALTER TABLE harvest_events ADD COLUMN x INT;"
        );
        // A computed name may also turn conforming strings off, which is a
        // finding of its own. So check the `ALTER` finding.
        let findings = lint_with_history(&[], &sql, true);
        assert!(
            findings
                .iter()
                .any(|f| f.detail.starts_with("ALTER TABLE locks harvest_events")),
            "{change}: {findings:?}"
        );
    }
    // A schema-qualified name still names the new table.
    let sql = "SET search_path = scratch;\nCREATE TABLE scratch.harvest_events (id INT);\n\
               SET search_path = public;\nALTER TABLE scratch.harvest_events ADD COLUMN x INT;";
    assert_eq!(lint_with_history(&[], sql, true), []);
}

#[test]
fn a_temporary_table_is_never_new() {
    // `ON COMMIT DROP` or the session end drops it. The name then means the
    // hot table again.
    for create in [
        "CREATE TEMP TABLE harvest_events (id INT) ON COMMIT DROP;",
        "CREATE TEMPORARY TABLE harvest_events (id INT);",
        "CREATE GLOBAL TEMPORARY TABLE harvest_events (id INT);",
        "CREATE LOCAL TEMP TABLE harvest_events (id INT);",
    ] {
        let sql = format!("{create}\nCOMMIT;\nALTER TABLE harvest_events ADD COLUMN x INT;");
        let findings = lint_with_history(&[], &sql, false);
        assert_eq!(
            rules(&findings),
            [Rule::LockTimeout],
            "{create}: {findings:?}"
        );
    }
    // A global temporary table still records its foreign keys.
    let history = [
        "CREATE GLOBAL TEMPORARY TABLE scratch (e UUID REFERENCES harvest_workflow_executions (id));",
    ];
    let findings = lint_with_history(&history, "DROP TABLE scratch;", true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn an_index_lives_in_the_schema_of_its_table() {
    // Postgres puts an index in its table's schema. A drop in another schema
    // names a different index, which the lint cannot place.
    let history = ["CREATE INDEX idx_shared ON staging.harvest_schedules (id);"];
    let set = "SET LOCAL lock_timeout = '5s';\n";
    for drop in ["DROP INDEX public.idx_shared;", "DROP INDEX idx_shared;"] {
        let findings = lint_with_history(&history, &format!("{set}{drop}"), true);
        assert_eq!(
            rules(&findings),
            [Rule::BlockingIndex],
            "{drop}: {findings:?}"
        );
    }
    let findings = lint_with_history(&history, "DROP INDEX staging.idx_shared;", true);
    assert_eq!(findings, []);
}

#[test]
fn a_non_ascii_dollar_tag_quotes_a_body() {
    // A tag follows the identifier rules, so it may hold any letter.
    let sql = "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $é$\n\
               BEGIN NULL; PERFORM set_config('lock_timeout', '5s', false); END $é$;\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn metadata_is_read_as_toml() {
    // A key inside a multiline string is text, not a key.
    assert!(run_in_transaction(
        "description = \"\"\"\nrun_in_transaction = false\n\"\"\"\n"
    ));
    assert!(!run_in_transaction("run_in_transaction = false\n"));
}

#[test]
fn a_sure_table_drop_forgets_its_indexes() {
    // The drop takes every index on the table with it.
    let history = [
        "CREATE INDEX idx_shared ON harvest_schedules (id);",
        "DROP TABLE harvest_schedules;",
    ];
    let sql = "SET LOCAL lock_timeout = '5s';\nDROP INDEX idx_shared;";
    let findings = lint_with_history(&history, sql, true);
    assert_eq!(rules(&findings), [Rule::BlockingIndex], "{findings:?}");
}

#[test]
fn concurrently_cannot_reach_a_partitioned_child() {
    // A child with `PARTITION BY` is a partitioned table too.
    let history = ["CREATE TABLE events_2026 PARTITION OF harvest_events \
                    FOR VALUES FROM (1) TO (2) PARTITION BY RANGE (id);"];
    let sql = "CREATE INDEX CONCURRENTLY idx_x ON events_2026 (id);";
    let findings = lint_with_history(&history, sql, false);
    assert_eq!(rules(&findings), [Rule::BlockingIndex], "{findings:?}");
    // A rename keeps the table partitioned.
    let renamed = [history[0], "ALTER TABLE events_2026 RENAME TO events_y;"];
    let sql = "CREATE INDEX CONCURRENTLY idx_x ON events_y (id);";
    let findings = lint_with_history(&renamed, sql, false);
    assert_eq!(rules(&findings), [Rule::BlockingIndex], "{findings:?}");
    // A leaf partition takes `CONCURRENTLY`.
    let leaf = ["CREATE TABLE events_p PARTITION OF harvest_events FOR VALUES FROM (1) TO (2);"];
    let sql = "CREATE INDEX CONCURRENTLY idx_x ON events_p (id);";
    assert_eq!(lint_with_history(&leaf, sql, false), []);
}

#[test]
fn a_set_config_name_is_case_insensitive() {
    let sql = "SET LOCAL lock_timeout = '5s';\n\
               SELECT set_config('LOCK_TIMEOUT', '0', true);\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn every_postgres_false_spelling_is_session_scope() {
    // A session clear outlives the commit. A local clear would not, and the
    // old session bound would come back.
    for is_local in ["'FALSE'", "'n'", "'Off'", "' no '", "'fal'", "FALSE"] {
        let sql = format!(
            "SET lock_timeout = '5s';\n\
             SELECT set_config('lock_timeout', '0', {is_local});\nCOMMIT;\n\
             ALTER TABLE harvest_events ADD COLUMN x INT;"
        );
        let findings = lint_with_history(&[], &sql, false);
        assert_eq!(
            rules(&findings),
            [Rule::LockTimeout],
            "{is_local}: {findings:?}"
        );
    }
}

#[test]
fn a_unicode_escaped_identifier_is_decoded() {
    for table in [
        r#"U&"harvest_events""#,
        r#"U&"harvest\005fevents""#,
        r#"u&"harvest\+00005fevents""#,
        r#"U&"harvest!005fevents" UESCAPE '!'"#,
    ] {
        let sql = format!("ALTER TABLE {table} ADD COLUMN x INT;");
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(
            rules(&findings),
            [Rule::LockTimeout],
            "{table}: {findings:?}"
        );
    }
}

#[test]
fn a_search_path_change_leaves_an_unqualified_index_unplaced() {
    // After the change, Postgres resolves the table through the new path. The
    // lint cannot follow it, so the index has no known schema.
    let set = "SET LOCAL lock_timeout = '5s';\n";
    let history =
        ["SET search_path = staging;\nCREATE INDEX idx_shared ON harvest_schedules (id);"];
    let findings = lint_with_history(
        &history,
        &format!("{set}DROP INDEX public.idx_shared;"),
        true,
    );
    assert_eq!(rules(&findings), [Rule::BlockingIndex], "{findings:?}");
    let history = ["CREATE INDEX idx_shared ON harvest_schedules (id);"];
    let sql = format!("SET search_path = staging;\n{set}DROP INDEX idx_shared;");
    let findings = lint_with_history(&history, &sql, true);
    assert_eq!(rules(&findings), [Rule::BlockingIndex], "{findings:?}");
}

#[test]
fn a_case_expression_in_a_do_body_does_not_split_the_statement() {
    let sql = "SET LOCAL lock_timeout = '5s';\nDO $$\nBEGIN\n    \
               ALTER TABLE harvest_events \
               ADD CHECK (CASE WHEN true THEN true ELSE false END), ADD UNIQUE (event_id);\n\
               END $$;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::BlockingIndex], "{findings:?}");
}

#[test]
fn a_dollar_literal_does_not_split_its_statement() {
    let sql = "SET LOCAL lock_timeout = '5s';\n\
               ALTER TABLE harvest_events ADD CHECK (note <> $$x$$), ADD UNIQUE (event_id);";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::BlockingIndex], "{findings:?}");
    // The statement after the literal is the same statement.
    let sql = "CREATE INDEX CONCURRENTLY idx_x ON harvest_task_queue (id) WHERE note <> $$x$$;";
    assert_eq!(lint_with_history(&[], sql, false), []);
}

#[test]
fn vacuum_full_false_is_a_plain_vacuum() {
    assert_eq!(
        lint_with_history(&[], "VACUUM (FULL false) harvest_events;", false),
        []
    );
    for full in ["FULL", "FULL true", "FULL on", "VERBOSE, FULL 1"] {
        let sql = format!("VACUUM ({full}) harvest_events;");
        let findings = lint_with_history(&[], &sql, false);
        assert_eq!(
            rules(&findings),
            [Rule::LockTimeout],
            "{full}: {findings:?}"
        );
    }
}

#[test]
fn a_named_argument_set_config_only_clears() {
    // A named call can clear the bound. The lint does not read it as a bound.
    for value in ["'0'", "'5s'"] {
        let sql = format!(
            "SET LOCAL lock_timeout = '5s';\n\
             SELECT set_config(setting_name => 'lock_timeout', new_value => {value}, \
             is_local => true);\n\
             ALTER TABLE harvest_events ADD COLUMN x INT;"
        );
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(
            rules(&findings),
            [Rule::LockTimeout],
            "{value}: {findings:?}"
        );
    }
}

#[test]
fn an_e_string_do_body_decodes_every_escape() {
    for table in [
        r"harvest\x5fevents",
        r"harvest\137events",
        r"harvest_events",
        r"harvest\U0000005fevents",
    ] {
        let sql = format!("DO E'BEGIN ALTER TABLE {table} ADD COLUMN x INT; END';");
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(
            rules(&findings),
            [Rule::LockTimeout],
            "{table}: {findings:?}"
        );
    }
}

#[test]
fn alter_index_rename_moves_the_history() {
    let set = "SET LOCAL lock_timeout = '5s';\n";
    let create = "CREATE INDEX idx_shared ON harvest_schedules (id);";
    for (moved, new_name) in [
        ("ALTER INDEX idx_shared RENAME TO idx_old;", "idx_old"),
        (
            "ALTER INDEX idx_shared SET SCHEMA staging;",
            "staging.idx_shared",
        ),
    ] {
        let history = [create, moved];
        // The old name is unknown now, so a drop of it fails closed.
        let findings = lint_with_history(&history, &format!("{set}DROP INDEX idx_shared;"), true);
        assert_eq!(
            rules(&findings),
            [Rule::BlockingIndex],
            "{moved}: {findings:?}"
        );
        // The new name keeps the cold table.
        let sql = format!("DROP INDEX {new_name};");
        assert_eq!(lint_with_history(&history, &sql, true), [], "{moved}");
    }
}

#[test]
fn execute_runs_its_constant_sql() {
    // PL/pgSQL `EXECUTE` runs the string, so the lint scans it as code.
    for body in [
        "EXECUTE 'ALTER TABLE harvest_events ADD COLUMN x INT';",
        "EXECUTE $q$ALTER TABLE harvest_events ADD COLUMN x INT$q$;",
        "EXECUTE format('ALTER TABLE %I ADD COLUMN x INT', 'harvest_events');",
    ] {
        let sql = format!("DO $$\nBEGIN\n    {body}\nEND $$;");
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(
            rules(&findings),
            [Rule::LockTimeout],
            "{body}: {findings:?}"
        );
    }
    // A placeholder is an unknown name, and an unknown index counts as hot.
    let sql = "SET LOCAL lock_timeout = '5s';\nDO $$\nBEGIN\n    \
               EXECUTE format('DROP INDEX %I.idx_x', 'staging');\nEND $$;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::BlockingIndex], "{findings:?}");
    // SQL with no lock passes.
    let sql = "DO $$\nBEGIN\n    EXECUTE 'SELECT 1';\nEND $$;";
    assert_eq!(lint_with_history(&[], sql, true), []);
}

#[test]
fn execute_of_sql_the_lint_cannot_read_fails_closed() {
    for body in [
        "EXECUTE 'ALTER TABLE ' || quote_ident('t') || ' ADD COLUMN x INT';",
        "EXECUTE q;",
        "EXECUTE concat('ALTER TABLE ', 'harvest_events ADD COLUMN x INT');",
    ] {
        let sql = format!("DO $$\nDECLARE q TEXT := 'SELECT 1';\nBEGIN\n    {body}\nEND $$;");
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(
            rules(&findings),
            [Rule::LockTimeout],
            "{body}: {findings:?}"
        );
    }
    // A top-level `EXECUTE` runs a prepared statement. Its arguments are data.
    let sql = "EXECUTE plan('ALTER TABLE harvest_events ADD COLUMN x INT');";
    assert_eq!(lint_with_history(&[], sql, true), []);
}

#[test]
fn a_dollar_literal_argument_is_a_string() {
    let sql = "SET LOCAL lock_timeout = '5s';\n\
               SELECT set_config($$lock_timeout$$, $$0$$, true);\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
    let sql = "SELECT set_config($$lock_timeout$$, $$5s$$, true);\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;";
    assert_eq!(lint_with_history(&[], sql, true), []);
}

#[test]
fn a_table_schema_move_forgets_its_indexes() {
    // Postgres moves the indexes with the table.
    let history = [
        "CREATE INDEX idx_shared ON public.harvest_schedules (id);",
        "ALTER TABLE public.harvest_schedules SET SCHEMA archive;",
    ];
    let sql = "SET LOCAL lock_timeout = '5s';\nDROP INDEX public.idx_shared;";
    let findings = lint_with_history(&history, sql, true);
    assert_eq!(rules(&findings), [Rule::BlockingIndex], "{findings:?}");
}

#[test]
fn a_format_text_placeholder_makes_execute_unreadable() {
    // `%s` inserts any text, so it can carry a whole statement.
    let sql = "DO $$\nDECLARE ddl TEXT := 'ALTER TABLE harvest_events ADD COLUMN x INT';\n\
               BEGIN\n    EXECUTE format('%s', ddl);\nEND $$;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn a_format_literal_placeholder_in_code_makes_execute_unreadable() {
    let set = "SET LOCAL lock_timeout = '5s';\n";
    let ddl = "'BEGIN ALTER TABLE harvest_events ADD COLUMN x INT; END'";
    // `%L` makes a string literal, and a `DO` or `EXECUTE` runs it as code.
    for template in [
        "'DO %L'",
        "'DO $x$ BEGIN EXECUTE %L; END $x$'",
        "'CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS %L'",
    ] {
        let sql = format!("{set}DO $$\nBEGIN\n    EXECUTE format({template}, {ddl});\nEND $$;");
        let findings = lint_with_history(&[], &sql, true);
        assert!(
            findings
                .iter()
                .any(|f| f.detail.starts_with("EXECUTE of SQL the lint cannot read")),
            "{sql}\n{findings:?}"
        );
    }
    // A `%L` value in a query is data.
    let sql = format!(
        "{set}DO $$\nBEGIN\n    EXECUTE format('SELECT %L', 'x');\nEND $$;\n\
         DO $$\nBEGIN\n    EXECUTE format('DO $x$ BEGIN PERFORM %L; END $x$', 'x');\nEND $$;"
    );
    assert_eq!(lint_with_history(&[], &sql, true), [], "{sql}");
}

#[test]
fn a_quoted_only_is_a_table_name() {
    let sql = "DROP TABLE IF EXISTS \"only\", harvest_events;";
    let findings = lint_with_history(&[], sql, true);
    assert!(
        findings.iter().any(|f| f.detail.contains("harvest_events")),
        "{findings:?}"
    );
}

#[test]
fn a_quoted_table_word_is_a_table_name() {
    for sql in [
        "LOCK \"table\", harvest_events;",
        "TRUNCATE \"table\", harvest_events;",
    ] {
        let findings = lint_with_history(&[], sql, true);
        assert!(
            findings.iter().any(|f| f.detail.contains("harvest_events")),
            "{sql}\n{findings:?}"
        );
    }
}

#[test]
fn a_routine_that_calls_a_locking_routine_locks() {
    let inner = "CREATE PROCEDURE inner_p() LANGUAGE plpgsql AS $$\nBEGIN\n    \
                 ALTER TABLE harvest_events ADD COLUMN y INT;\nEND $$;";
    let middle = "CREATE FUNCTION middle_f() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    \
                  CALL inner_p();\nEND $$;";
    let outer = "CREATE FUNCTION outer_f() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    \
                 PERFORM middle_f();\nEND $$;";
    let one_file = format!("{inner}\n{middle}\n{outer}");
    // The lock passes up the call chain, in one file or across files.
    for history in [vec![one_file.as_str()], vec![inner, middle, outer]] {
        let findings = lint_with_history(&history, "SELECT outer_f();", true);
        assert_eq!(
            rules(&findings),
            [Rule::LockTimeout],
            "{history:?}\n{findings:?}"
        );
    }
}

#[test]
fn a_routine_that_calls_a_clearing_routine_clears() {
    let inner = "CREATE PROCEDURE inner_p() LANGUAGE plpgsql AS $$\nBEGIN\n    \
                 PERFORM set_config('lock_timeout', '0', false);\nEND $$;";
    let middle = "CREATE FUNCTION middle_f() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    \
                  CALL inner_p();\nEND $$;";
    let outer = "CREATE FUNCTION outer_f() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    \
                 PERFORM middle_f();\nEND $$;";
    let one_file = format!("{inner}\n{middle}\n{outer}");
    let sql = "SET LOCAL lock_timeout = '5s';\nSELECT outer_f();\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;";
    let unbounded = |findings: &[Finding]| {
        findings
            .iter()
            .any(|f| f.detail.starts_with("ALTER TABLE locks harvest_events"))
    };
    // The clear passes up the call chain, in one file or across files.
    for history in [vec![one_file.as_str()], vec![inner, middle, outer]] {
        let findings = lint_with_history(&history, sql, true);
        assert!(unbounded(&findings), "{history:?}\n{findings:?}");
    }
    // In the same file, the call clears the bound too.
    let sql = format!(
        "{one_file}\nSET LOCAL lock_timeout = '5s';\nSELECT outer_f();\n\
         ALTER TABLE harvest_events ADD COLUMN x INT;"
    );
    let findings = lint_with_history(&[], &sql, true);
    assert!(unbounded(&findings), "{findings:?}");
}

#[test]
fn a_quoted_column_name_is_no_drop_clause() {
    // `"default"` names a column here, and its key references a hot table.
    let history =
        ["CREATE TABLE cold (id INT, \"default\" BIGINT REFERENCES harvest_events (id));"];
    let sql = "ALTER TABLE cold DROP \"default\";";
    let findings = lint_with_history(&history, sql, true);
    assert!(
        findings.iter().any(|f| f.detail.contains("harvest_events")),
        "{findings:?}"
    );
}

#[test]
fn a_quoted_statement_head_does_not_make_code() {
    // `"do"` and `"execute"` name variables here, so each value is data.
    for name in ["do", "execute"] {
        let sql = format!(
            "DO $$\nDECLARE \"{name}\" text;\nBEGIN\n    \
             \"{name}\" := $sql$ALTER TABLE harvest_events ADD COLUMN x INT$sql$;\n    \
             \"{name}\" := 'ALTER TABLE harvest_events ADD COLUMN y INT';\nEND $$;"
        );
        assert_eq!(lint_with_history(&[], &sql, true), [], "{sql}");
    }
}

#[test]
fn a_quoted_case_is_no_case_expression() {
    // `"case"` names a variable, so the `THEN` still starts the `ALTER`.
    let sql = "DO $$\nBEGIN\n    IF \"case\" THEN\n        \
               ALTER TABLE harvest_events ADD COLUMN x INT;\n    END IF;\nEND $$;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn a_quoted_label_does_not_end_a_branch() {
    // `END "if"` closes a block labelled `if`, not the `IF` around it.
    let sql = "DO $$\nBEGIN\n    IF random() < 0.5 THEN\n        <<\"if\">>\n        BEGIN\n            \
               NULL;\n        END \"if\";\n        SET LOCAL lock_timeout = '5s';\n    END IF;\n\
               END $$;\nALTER TABLE harvest_events ADD COLUMN x INT;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn a_foreign_routine_body_is_not_sql() {
    // Postgres only stores the source. A call of the routine counts instead.
    let sql = "CREATE FUNCTION f() RETURNS void LANGUAGE plpython3u AS $$\n\
               # ; ALTER TABLE harvest_events ADD COLUMN x INT\npass\n$$;";
    assert_eq!(lint_with_history(&[], sql, true), [], "{sql}");
    let sql = format!("{sql}\nSET LOCAL lock_timeout = '5s';\nSELECT f();");
    let findings = lint_with_history(&[], &sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn foreign_routine_text_changes_no_setting() {
    let routine = "CREATE FUNCTION f() RETURNS void LANGUAGE plpython3u AS $$\n\
                   # ; SET standard_conforming_strings = off;\npass\n$$;";
    let hidden = "SET LOCAL lock_timeout = '5s';\nDO 'BEGIN ALTER TABLE harvest_event\\163 ADD COLUMN x INT; END';";
    // Postgres only stores the source, so the setting stays on.
    assert_eq!(lint_with_history(&[routine], hidden, true), [], "{hidden}");
    let sql = format!("{routine}\n{hidden}");
    assert_eq!(lint_with_history(&[], &sql, true), [], "{sql}");
}

#[test]
fn a_routine_setting_covers_an_atomic_body() {
    // An atomic body holds no DDL, but it may call a routine that locks.
    let history = [
        "CREATE FUNCTION legacy_f() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    \
                    ALTER TABLE harvest_events ADD COLUMN y INT;\nEND $$;",
    ];
    let body = "BEGIN ATOMIC\n    SELECT legacy_f();\nEND;";
    let sql =
        format!("CREATE FUNCTION f() RETURNS void LANGUAGE sql SET lock_timeout = '5s'\n{body}");
    assert_eq!(lint_with_history(&history, &sql, true), [], "{sql}");
    let sql = format!("CREATE FUNCTION f() RETURNS void LANGUAGE sql\n{body}");
    let findings = lint_with_history(&history, &sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn a_body_change_at_the_body_start_follows_the_routine_setting() {
    // Postgres applies the clause first, so the body's `RESET` clears it.
    let sql = "CREATE FUNCTION f() RETURNS void LANGUAGE sql SET lock_timeout = '5s' \
               AS 'RESET lock_timeout; ALTER TABLE harvest_events ADD COLUMN x INT';";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn an_outer_routine_bound_does_not_cover_an_inner_routine() {
    let inner = "    CREATE FUNCTION inner_f() RETURNS void LANGUAGE plpgsql AS $i$\n    BEGIN\n        \
                 ALTER TABLE harvest_events ADD COLUMN x INT;\n    END $i$;\n";
    // The inner routine runs later, after the outer call has restored its value.
    for (clause, setter) in [
        ("SET lock_timeout = '5s' ", ""),
        ("", "    SET LOCAL lock_timeout = '5s';\n"),
    ] {
        let sql = format!(
            "CREATE FUNCTION outer_f() RETURNS void LANGUAGE plpgsql {clause}AS $o$\nBEGIN\n\
             {setter}{inner}END $o$;"
        );
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}\n{findings:?}");
    }
    // A bound in the inner body covers its lock.
    let sql = "CREATE FUNCTION outer_f() RETURNS void LANGUAGE plpgsql AS $o$\nBEGIN\n    \
               CREATE FUNCTION inner_f() RETURNS void LANGUAGE plpgsql AS $i$\n    BEGIN\n        \
               SET LOCAL lock_timeout = '5s';\n        ALTER TABLE harvest_events ADD COLUMN x INT;\n    \
               END $i$;\nEND $o$;";
    assert_eq!(lint_with_history(&[], sql, true), [], "{sql}");
    // The SQL of an `EXECUTE` in a body belongs to that body.
    let sql = "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    \
               SET LOCAL lock_timeout = '5s';\n    \
               EXECUTE 'ALTER TABLE harvest_events ADD COLUMN x INT';\nEND $$;";
    assert_eq!(lint_with_history(&[], sql, true), [], "{sql}");
}

#[test]
fn a_routine_setting_covers_a_lock_at_the_body_start() {
    // A SQL body may start with the lock itself.
    let sql = "CREATE FUNCTION f() RETURNS void LANGUAGE sql SET lock_timeout = '5s' \
               AS 'ALTER TABLE harvest_events ADD COLUMN x INT';";
    assert_eq!(lint_with_history(&[], sql, true), [], "{sql}");
    let sql = "CREATE FUNCTION f() RETURNS void LANGUAGE sql \
               AS 'ALTER TABLE harvest_events ADD COLUMN x INT';";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn a_bound_from_an_earlier_migration_does_not_count() {
    // A migration may run alone on a new connection, without the earlier SET.
    let earlier = "SET lock_timeout = '5s';";
    let sql = "ALTER TABLE harvest_events ADD COLUMN x INT;";
    let findings = lint_with_history(&[earlier], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn a_set_in_a_routine_signature_is_no_setting() {
    // `"set"` names a parameter here, so the routine sets no timeout.
    let sql = "CREATE FUNCTION f(\"set\" lock_timeout = '5s') RETURNS void LANGUAGE plpgsql AS $$\n\
               BEGIN\n    ALTER TABLE harvest_events ADD COLUMN x INT;\nEND $$;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
    // The clause after the signature still sets one.
    let sql = "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql SET lock_timeout = '5s' AS $$\n\
               BEGIN\n    ALTER TABLE harvest_events ADD COLUMN x INT;\nEND $$;";
    assert_eq!(lint_with_history(&[], sql, true), [], "{sql}");
}

#[test]
fn a_quoted_dot_is_part_of_the_name() {
    // `"public.harvest_events"` is one name, not a schema and a table.
    let sql = "CREATE TABLE \"public.harvest_events\" (id INT);\n\
               ALTER TABLE public.harvest_events ADD COLUMN x INT;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
    let findings = lint_with_history(
        &[],
        "ALTER TABLE \"harvest_events\" ADD COLUMN x INT;",
        true,
    );
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn an_unparsed_set_config_of_lock_timeout_clears() {
    for call in [
        "set_config('lock_timeout'::text, '0', true)",
        "set_config(('lock_timeout'), '0', true)",
    ] {
        let sql = format!(
            "SET LOCAL lock_timeout = '5s';\nSELECT {call};\n\
             ALTER TABLE harvest_events ADD COLUMN x INT;"
        );
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(
            rules(&findings),
            [Rule::LockTimeout],
            "{call}: {findings:?}"
        );
    }
}

#[test]
fn an_alter_that_drops_dependents_forgets_the_indexes() {
    // A dropped column or constraint takes its indexes with it.
    for alter in [
        "ALTER TABLE harvest_schedules DROP COLUMN c;",
        "ALTER TABLE harvest_schedules DROP CONSTRAINT harvest_schedules_c_key;",
    ] {
        let history = ["CREATE INDEX idx_shared ON harvest_schedules (c);", alter];
        let sql = "SET LOCAL lock_timeout = '5s';\nDROP INDEX idx_shared;";
        let findings = lint_with_history(&history, sql, true);
        assert_eq!(
            rules(&findings),
            [Rule::BlockingIndex],
            "{alter}: {findings:?}"
        );
    }
}

#[test]
fn drop_schema_cascade_locks_an_unknown_table() {
    // CASCADE drops every table in the schema, hot ones included.
    let findings = lint_with_history(&[], "DROP SCHEMA public CASCADE;", true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
    // It also drops every index there, so a cold mapping goes stale.
    let history = [
        "CREATE INDEX idx_shared ON harvest_schedules (id);",
        "DROP SCHEMA public CASCADE;",
    ];
    let sql = "SET LOCAL lock_timeout = '5s';\nDROP INDEX idx_shared;";
    let findings = lint_with_history(&history, sql, true);
    assert_eq!(rules(&findings), [Rule::BlockingIndex], "{findings:?}");
}

#[test]
fn a_begin_atomic_body_does_not_run() {
    // `CREATE FUNCTION` only stores the body. Its setter sets nothing.
    let sql = "CREATE FUNCTION f() RETURNS void LANGUAGE sql\nBEGIN ATOMIC\n    SELECT 1;\n    \
               SELECT set_config('lock_timeout', '5s', false);\nEND;\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn adjacent_string_literals_join() {
    // Postgres joins two literals that only whitespace with a newline parts.
    let sql = "DO $$\nBEGIN\n    EXECUTE 'ALTER TABLE harvest_'\n        'events ADD COLUMN x INT';\nEND $$;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
    assert!(
        findings[0].detail.contains("harvest_events"),
        "{findings:?}"
    );
}

#[test]
fn a_computed_set_config_name_may_clear() {
    let sql = "SET lock_timeout = '5s';\n\
               SELECT set_config('lock_' || 'timeout', '0', false);\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;";
    // The computed name may also turn conforming strings off, which is a
    // finding of its own.
    let findings = lint_with_history(&[], sql, false);
    assert!(
        findings
            .iter()
            .any(|f| f.detail.starts_with("ALTER TABLE locks harvest_events")),
        "{findings:?}"
    );
    // A literal name of another setting leaves the bound alone.
    let sql = "SET LOCAL lock_timeout = '5s';\n\
               SELECT set_config('statement_timeout', '0', true);\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;";
    assert_eq!(lint_with_history(&[], sql, true), []);
}

#[test]
fn a_single_quoted_function_body_is_scanned() {
    // Its locks count, as in a dollar-quoted body.
    for quote in ["", "E", "U&"] {
        let sql = format!(
            "CREATE FUNCTION f() RETURNS void AS \
             {quote}'BEGIN ALTER TABLE harvest_events ADD COLUMN x INT; END' LANGUAGE plpgsql;"
        );
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(
            rules(&findings),
            [Rule::LockTimeout],
            "{quote}: {findings:?}"
        );
    }
}

#[test]
fn other_cascading_drops_lock_an_unknown_table() {
    // A dependent column or default may sit on a hot table.
    for sql in [
        "DROP TYPE that_type CASCADE;",
        "DROP DOMAIN d CASCADE;",
        "DROP FUNCTION f() CASCADE;",
        "DROP SEQUENCE s CASCADE;",
    ] {
        let findings = lint_with_history(&[], sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}: {findings:?}");
    }
    assert_eq!(lint_with_history(&[], "DROP TYPE that_type;", true), []);
}

#[test]
fn a_lock_in_a_function_body_needs_a_bound_in_the_body() {
    // The body runs when something calls the function, maybe much later.
    // The migration's bound does not hold then, and a clear in the body counts.
    let body = |inner: &str| {
        format!(
            "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n{inner}\n    \
             ALTER TABLE harvest_events ADD COLUMN x INT;\nEND $$;\nSELECT f();"
        )
    };
    for sql in [
        format!(
            "SET LOCAL lock_timeout = '5s';\n{}",
            body("    PERFORM set_config('lock_timeout', '0', true);")
        ),
        format!("SET LOCAL lock_timeout = '5s';\n{}", body("")),
    ] {
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}: {findings:?}");
    }
    // A bound set in the body holds for the lock after it.
    let sql = body("    PERFORM set_config('lock_timeout', '5s', true);");
    assert_eq!(lint_with_history(&[], &sql, true), []);
}

#[test]
fn reindex_concurrently_reads_a_quoted_true() {
    for value in ["'true'", "'on'", "TRUE"] {
        let sql = format!("REINDEX (CONCURRENTLY {value}) TABLE harvest_task_queue;");
        assert_eq!(lint_with_history(&[], &sql, false), [], "{value}");
    }
}

#[test]
fn a_clear_is_local_only_with_a_plain_true_scope() {
    // A session clear outlives the commit. A cast hides the scope, so the
    // lint reads it as session.
    let sql = "SET lock_timeout = '5s';\n\
               SELECT set_config('lock_timeout', '0', false::boolean);\nCOMMIT;\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;";
    let findings = lint_with_history(&[], sql, false);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn a_commit_in_a_do_body_ends_the_transaction() {
    for end in ["COMMIT", "ROLLBACK"] {
        let sql = format!(
            "DO $$\nBEGIN\n    PERFORM set_config('lock_timeout', '5s', true);\n    {end};\n    \
             ALTER TABLE harvest_events ADD COLUMN x INT;\nEND $$;"
        );
        let findings = lint_with_history(&[], &sql, false);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{end}: {findings:?}");
    }
}

#[test]
fn unreadable_execute_may_clear_the_bound() {
    // The hidden SQL may run `RESET lock_timeout` before any lock.
    let sql = "DO $$\nDECLARE ddl TEXT := 'RESET lock_timeout';\nBEGIN\n    \
               PERFORM set_config('lock_timeout', '5s', true);\n    EXECUTE ddl;\n    \
               ALTER TABLE harvest_events ADD COLUMN x INT;\nEND $$;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(
        rules(&findings),
        [Rule::LockTimeout, Rule::LockTimeout],
        "{findings:?}"
    );
    assert!(
        findings.iter().any(|f| f.detail.contains("harvest_events")),
        "{findings:?}"
    );
}

#[test]
fn a_commit_in_a_procedure_body_ends_its_bound() {
    // `CALL` runs the body, and a procedure may commit.
    let sql = "CREATE PROCEDURE p() LANGUAGE plpgsql AS $$\nBEGIN\n    \
               PERFORM set_config('lock_timeout', '5s', true);\n    COMMIT;\n    \
               ALTER TABLE harvest_events ADD COLUMN x INT;\nEND $$;\nCALL p();";
    let findings = lint_with_history(&[], sql, false);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn a_continued_unicode_literal_joins() {
    let sql = "DO U&'BEGIN ALTER TABLE harvest_'\n    'events ADD COLUMN x INT; END';";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
    assert!(
        findings[0].detail.contains("harvest_events"),
        "{findings:?}"
    );
}

#[test]
fn a_schema_move_ends_a_new_table_exemption() {
    // The unqualified name can mean the hot table once the new one moves.
    let sql = "CREATE TABLE harvest_events (id INT);\n\
               ALTER TABLE harvest_events SET SCHEMA archive;\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;";
    let findings = lint_with_history(&[], sql, true);
    assert!(
        findings
            .iter()
            .any(|f| f.rule == Rule::LockTimeout && f.line == 3),
        "{findings:?}"
    );
}

#[test]
fn a_nested_handler_rolls_back_only_its_block() {
    // The handler undoes its own block. The outer setter and lock stay.
    let sql = "DO $$\nBEGIN\n    PERFORM set_config('lock_timeout', '5s', true);\n    BEGIN\n        \
               PERFORM 1 / 0;\n    EXCEPTION WHEN others THEN\n        NULL;\n    END;\n    \
               ALTER TABLE harvest_events ADD COLUMN x INT;\nEND $$;";
    assert_eq!(lint_with_history(&[], sql, true), []);
    // A setter inside the handled block may still be undone.
    let sql = "DO $$\nBEGIN\n    BEGIN\n        PERFORM set_config('lock_timeout', '5s', true);\n    \
               EXCEPTION WHEN others THEN\n        NULL;\n    END;\n    \
               ALTER TABLE harvest_events ADD COLUMN x INT;\nEND $$;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn a_pg_catalog_setter_is_a_bare_call() {
    let sql = "SELECT pg_catalog.set_config('lock_timeout', '5s', true);\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;";
    assert_eq!(lint_with_history(&[], sql, true), []);
    // Another schema may hold a different function.
    let sql = "SELECT app.set_config('lock_timeout', '5s', true);\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn a_literal_continues_across_a_line_comment() {
    for literal in ["'ALTER TABLE harvest_'", "U&'ALTER TABLE harvest_'"] {
        let sql = format!(
            "DO $$\nBEGIN\n    EXECUTE {literal} -- continued\n        \
             'events ADD COLUMN x INT';\nEND $$;"
        );
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
        assert!(
            findings[0].detail.contains("harvest_events"),
            "{findings:?}"
        );
    }
}

#[test]
fn a_function_set_clause_bounds_its_body() {
    // Postgres applies the clause each time the function runs.
    let body = "$$\nBEGIN\n    ALTER TABLE harvest_events ADD COLUMN x INT;\nEND $$";
    for sql in [
        format!(
            "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql SET lock_timeout TO '5s' AS {body};"
        ),
        format!(
            "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS {body} SET lock_timeout = '5s';"
        ),
    ] {
        assert_eq!(lint_with_history(&[], &sql, true), [], "{sql}");
    }
    let sql = format!(
        "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql SET lock_timeout = '0' AS {body};"
    );
    let findings = lint_with_history(&[], &sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn a_continued_e_string_keeps_its_escapes() {
    // `\\163` decodes to `s` only while the escapes hold.
    let sql = "DO $$\nBEGIN\n    EXECUTE E'ALTER TABLE harvest_event'\n        \
               '\\163 ADD COLUMN x INT';\nEND $$;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
    assert!(
        findings[0].detail.contains("harvest_events"),
        "{findings:?}"
    );
}

#[test]
fn a_call_may_clear_the_bound() {
    let set = "SET LOCAL lock_timeout = '5s';\n";
    let lock = "ALTER TABLE harvest_events ADD COLUMN x INT;";
    let clears = "BEGIN\n    PERFORM set_config('lock_timeout', '0', true);\nEND";
    let keeps = "BEGIN\n    RAISE NOTICE 'hi';\nEND";
    let procedure =
        |body: &str| format!("CREATE PROCEDURE p() LANGUAGE plpgsql AS $$\n{body} $$;\n");
    let function =
        format!("CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$\n{clears} $$;\n");
    // A body that clears may clear the bound.
    for sql in [
        format!("{set}{}CALL p();\n{lock}", procedure(clears)),
        format!("{set}{function}SELECT f();\n{lock}"),
    ] {
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}");
    }
    // An unread body is an unbounded lock, and it may clear the bound.
    let sql = format!("{set}CALL p();\n{lock}");
    let findings = lint_with_history(&[], &sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout; 2], "{sql}");
    // A known body with no timeout change keeps the bound.
    let sql = format!("{set}{}CALL p();\n{lock}", procedure(keeps));
    assert_eq!(lint_with_history(&[], &sql, true), [], "{sql}");
}

#[test]
fn a_commit_ends_a_new_table() {
    for path in [
        "SET LOCAL search_path = scratch, public;",
        "SELECT set_config('search_path', 'scratch, public', true);",
    ] {
        for end in ["COMMIT;", "END;"] {
            let sql = format!(
                "{path}\nCREATE TABLE harvest_events (id BIGINT);\n{end}\n\
                 ALTER TABLE harvest_events ADD COLUMN x INT;"
            );
            let findings = lint_with_history(&[], &sql, true);
            assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}");
        }
    }
    // Other sessions can lock a committed table, so a commit ends the
    // exemption without a `search_path` change too.
    for end in ["COMMIT;", "END;"] {
        let sql = format!(
            "CREATE TABLE harvest_events (id BIGINT);\n{end}\n\
             ALTER TABLE harvest_events ADD COLUMN x INT;"
        );
        let findings = lint_with_history(&[], &sql, false);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}");
    }
}

#[test]
fn an_index_tablespace_clause_still_builds_an_index() {
    let set = "SET LOCAL lock_timeout = '5s';\n";
    for action in [
        "ADD UNIQUE (id) USING INDEX TABLESPACE fast",
        "ADD CONSTRAINT k PRIMARY KEY (id) USING INDEX TABLESPACE fast",
    ] {
        let sql = format!("{set}ALTER TABLE harvest_events {action};");
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(rules(&findings), [Rule::BlockingIndex], "{sql}");
    }
    // `USING INDEX` with an index name adopts that index.
    let sql = format!("{set}ALTER TABLE harvest_events ADD CONSTRAINT k UNIQUE USING INDEX k_idx;");
    assert_eq!(lint_with_history(&[], &sql, true), [], "{sql}");
}

#[test]
fn a_drop_that_may_remove_a_new_table_ends_its_exemption() {
    let create = "CREATE TABLE harvest_events (id BIGINT);\n";
    let lock = "ALTER TABLE harvest_events ADD COLUMN x INT;";
    for drop in [
        "DROP SCHEMA scratch CASCADE;",
        "DROP SCHEMA IF EXISTS scratch;",
        "DROP OWNED BY scratch_owner;",
        "DROP TABLE scratch.harvest_events;",
    ] {
        let sql =
            format!("{create}SET LOCAL lock_timeout = '5s';\n{drop}\nRESET lock_timeout;\n{lock}");
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}");
    }
    // A qualified create ends at an unqualified drop too.
    let sql = "CREATE TABLE public.harvest_events (id BIGINT);\n\
               SET LOCAL lock_timeout = '5s';\nDROP TABLE harvest_events;\nRESET lock_timeout;\n\
               ALTER TABLE public.harvest_events ADD COLUMN x INT;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}");
}

#[test]
fn a_quoted_name_is_not_a_keyword() {
    for (quoted, plain) in [
        (
            "CREATE INDEX \"concurrently\" ON harvest_task_queue (scheduled_at);",
            "CREATE INDEX idx ON harvest_task_queue (scheduled_at);",
        ),
        (
            "CREATE INDEX \"on\" ON harvest_task_queue (scheduled_at);",
            "CREATE INDEX idx ON harvest_task_queue (scheduled_at);",
        ),
        ("DROP INDEX \"concurrently\";", "DROP INDEX idx;"),
    ] {
        let history = [
            "CREATE INDEX \"concurrently\" ON harvest_task_queue (id);",
            "CREATE INDEX idx ON harvest_task_queue (id);",
        ];
        let expected = rules(&lint_with_history(&history, plain, false));
        assert!(!expected.is_empty(), "{plain}");
        assert_eq!(
            rules(&lint_with_history(&history, quoted, false)),
            expected,
            "{quoted}"
        );
    }
}

#[test]
fn an_execute_expression_clears_before_its_sql_runs() {
    let sql = "SET LOCAL lock_timeout = '5s';\nDO $$\nBEGIN\n    EXECUTE format(\
               'ALTER TABLE harvest_events ADD COLUMN x INT /* %L */', \
               set_config('lock_timeout', '0', true));\nEND $$;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn a_call_matches_a_routine_only_by_its_full_name_and_arity() {
    let set = "SET LOCAL lock_timeout = '5s';\n";
    let lock = "ALTER TABLE harvest_events ADD COLUMN x INT;";
    let keeps = "CREATE PROCEDURE p(a INT) LANGUAGE plpgsql AS $$\nBEGIN\n    RAISE NOTICE 'hi';\nEND $$;\n";
    // Another schema, another arity, or a call before the create may reach another routine.
    // Each unread call is an unbounded lock, and the ALTER loses its bound.
    for call in ["CALL other.p(1);", "CALL p();", "CALL p(1, 2);"] {
        let sql = format!("{set}{keeps}{call}\n{lock}");
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout; 2], "{sql}");
    }
    let sql = format!("{set}CALL p(1);\n{keeps}{lock}");
    let findings = lint_with_history(&[], &sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout; 2], "{sql}");
    // The same name and arity after the create reach the known body.
    let sql = format!("{set}{keeps}CALL p(1);\n{lock}");
    assert_eq!(lint_with_history(&[], &sql, true), [], "{sql}");
}

#[test]
fn a_text_placeholder_in_quoted_template_text_is_unreadable() {
    for template in [
        "'ALTER TABLE \"%s\"'",
        "'ALTER TABLE t -- %s'",
        "'ALTER TABLE t /* %1$s */'",
    ] {
        let sql = format!(
            "SET LOCAL lock_timeout = '5s';\nDO $$\nBEGIN\n    EXECUTE format({template}, 'x');\nEND $$;"
        );
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}");
        assert!(findings[0].detail.contains("cannot read"), "{findings:?}");
    }
}

#[test]
fn a_call_keeps_the_bound_only_when_no_overload_clears() {
    let set = "SET LOCAL lock_timeout = '5s';\n";
    let lock = "ALTER TABLE harvest_events ADD COLUMN x INT;";
    let clears = "CREATE PROCEDURE p(a TEXT) LANGUAGE plpgsql AS $$\nBEGIN\n    \
                  PERFORM set_config('lock_timeout', '0', true);\nEND $$;\n";
    let keeps = "CREATE PROCEDURE p(a INT) LANGUAGE plpgsql AS $$\nBEGIN\n    RAISE NOTICE 'hi';\nEND $$;\n";
    let sql = format!("{set}{clears}{keeps}CALL p('x'::text);\n{lock}");
    let findings = lint_with_history(&[], &sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}");
}

#[test]
fn an_unqualified_set_config_after_a_path_change_sets_no_bound() {
    let path = "SET search_path = app, pg_catalog;\n";
    let lock = "ALTER TABLE harvest_events ADD COLUMN x INT;";
    // `app.set_config` may shadow the built-in.
    let sql = format!("{path}SELECT set_config('lock_timeout', '5s', true);\n{lock}");
    let findings = lint_with_history(&[], &sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}");
    let sql = format!("{path}SELECT pg_catalog.set_config('lock_timeout', '5s', true);\n{lock}");
    assert_eq!(lint_with_history(&[], &sql, true), [], "{sql}");
}

#[test]
fn a_do_body_in_another_language_is_unreadable() {
    let body = "$$ plpy.execute(\"ALTER TABLE harvest_events ADD COLUMN x int\") $$";
    for sql in [
        format!("DO LANGUAGE plpython3u {body};"),
        format!("DO {body} LANGUAGE 'plpython3u';"),
    ] {
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}");
        assert!(findings[0].detail.contains("unknown"), "{findings:?}");
    }
    // The foreign body may also clear the bound for what comes after it.
    let sql = format!(
        "SET LOCAL lock_timeout = '5s';\nDO LANGUAGE plpython3u {body};\n\
         ALTER TABLE harvest_events ADD COLUMN y INT;"
    );
    let findings = lint_with_history(&[], &sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout; 2], "{sql}");
    assert!(
        findings[1].detail.contains("harvest_events"),
        "{findings:?}"
    );
    // An explicit PL/pgSQL body is still read.
    let sql = "DO LANGUAGE plpgsql $$\nBEGIN\n    NULL;\nEND $$;";
    assert_eq!(lint_with_history(&[], sql, true), [], "{sql}");
}

#[test]
fn a_clearing_function_from_an_earlier_migration_clears() {
    let earlier = "CREATE FUNCTION clear_timeout() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    \
                   PERFORM set_config('lock_timeout', '0', true);\nEND $$;";
    let set = "SET LOCAL lock_timeout = '5s';\n";
    let lock = "ALTER TABLE harvest_events ADD COLUMN x INT;";
    for call in ["SELECT clear_timeout();", "PERFORM clear_timeout();"] {
        let sql = if call.starts_with("PERFORM") {
            format!("{set}DO $$\nBEGIN\n    {call}\nEND $$;\n{lock}")
        } else {
            format!("{set}{call}\n{lock}")
        };
        let findings = lint_with_history(&[earlier], &sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}");
    }
}

#[test]
fn foreign_code_never_inherits_a_bound() {
    let set = "SET LOCAL lock_timeout = '5s';\n";
    let body = "$$ plpy.execute(\"ALTER TABLE harvest_events ADD COLUMN x int\") $$";
    let function = format!("CREATE FUNCTION f() RETURNS void LANGUAGE plpython3u AS {body};\n");
    // A foreign body may clear the bound before it locks.
    let sql = format!("{set}DO LANGUAGE plpython3u {body};");
    let findings = lint_with_history(&[], &sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}");
    // A call of a foreign routine runs foreign code.
    for (history, sql) in [
        (vec![], format!("{function}{set}SELECT f();")),
        (vec![function.as_str()], format!("{set}SELECT f();")),
        (
            vec![],
            format!("{function}{set}DO $$\nBEGIN\n    PERFORM f();\nEND $$;"),
        ),
    ] {
        let findings = lint_with_history(&history, &sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}");
        assert!(findings[0].detail.contains("unknown"), "{findings:?}");
    }
    // A SQL routine is still read.
    let sql = format!(
        "CREATE FUNCTION g() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;\n{set}SELECT g();"
    );
    assert_eq!(lint_with_history(&[], &sql, true), [], "{sql}");
}

#[test]
fn an_execute_in_a_branch_sets_no_bound() {
    let lock = "ALTER TABLE harvest_events ADD COLUMN x INT;";
    let set = "EXECUTE 'SET LOCAL lock_timeout = ''5s''';";
    for body in [
        format!("IF false THEN\n        {set}\n    END IF;"),
        format!("LOOP\n        {set}\n        EXIT;\n    END LOOP;"),
        format!("RETURN;\n    {set}"),
    ] {
        let sql = format!("DO $$\nBEGIN\n    {body}\nEND $$;\n{lock}");
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}");
    }
    // An unconditional EXECUTE still sets the bound.
    let sql = format!("DO $$\nBEGIN\n    {set}\nEND $$;\n{lock}");
    assert_eq!(lint_with_history(&[], &sql, true), [], "{sql}");
}

#[test]
fn a_quoted_language_name_keeps_its_case() {
    let body = "$$ BEGIN NULL; END $$";
    // A quoted name is not folded, so `"PLPGSQL"` is another language.
    let sql = format!("DO LANGUAGE \"PLPGSQL\" {body};");
    let findings = lint_with_history(&[], &sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}");
    let sql = "CREATE FUNCTION f() RETURNS int LANGUAGE \"SQL\" AS $$ SELECT 1 $$;\nSELECT f();";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}");
    // An unquoted name is folded, so `PLPGSQL` is PL/pgSQL.
    let sql = format!("DO LANGUAGE PLPGSQL {body};");
    assert_eq!(lint_with_history(&[], &sql, true), [], "{sql}");
}

#[test]
fn a_uescape_clause_follows_a_comment() {
    for gap in [
        "/* gap */ UESCAPE '!'",
        "-- gap\n UESCAPE '!'",
        "UESCAPE /* gap */ '!'",
    ] {
        let sql = format!("ALTER TABLE U&\"harvest_!0065vents\" {gap} ADD COLUMN x INT;");
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}");
        assert!(
            findings[0].detail.contains("harvest_events"),
            "{findings:?}"
        );
    }
}

#[test]
fn the_language_clause_follows_the_routine_signature() {
    let set = "SET LOCAL lock_timeout = '5s';\n";
    for create in [
        "CREATE FUNCTION language() RETURNS void LANGUAGE plpython3u AS $$ pass $$;",
        "CREATE FUNCTION public.language() RETURNS void LANGUAGE plpython3u AS $$ pass $$;",
        "CREATE FUNCTION f(language text) RETURNS void LANGUAGE plpython3u AS $$ pass $$;",
    ] {
        let call = if create.contains("f(") {
            "SELECT f('x');"
        } else {
            "SELECT language();"
        };
        let sql = format!("{create}\n{set}{call}");
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}");
    }
    // A parameter named `language` is not the clause.
    let sql = format!(
        "CREATE FUNCTION g(language text) RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;\n{set}SELECT g('x');"
    );
    assert_eq!(lint_with_history(&[], &sql, true), [], "{sql}");
}

#[test]
fn a_composed_format_template_is_unreadable() {
    for template in [
        "format('ALTER TABLE harvest_' || 'events ADD COLUMN x INT')",
        "format('ALTER TABLE harvest_' || 'events ADD COLUMN %I INT', 'x')",
    ] {
        let sql = format!(
            "SET LOCAL lock_timeout = '5s';\nDO $$\nBEGIN\n    EXECUTE {template};\nEND $$;"
        );
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}");
        assert!(findings[0].detail.contains("cannot read"), "{findings:?}");
    }
    // One literal, with or without arguments, stays readable.
    for template in [
        "format('ALTER TABLE t ADD COLUMN x INT')",
        "format('ALTER TABLE t ADD COLUMN %I INT', 'x')",
    ] {
        let sql = format!(
            "SET LOCAL lock_timeout = '5s';\nDO $$\nBEGIN\n    EXECUTE {template};\nEND $$;"
        );
        assert_eq!(lint_with_history(&[], &sql, true), [], "{sql}");
    }
}

#[test]
fn an_unqualified_call_after_a_path_change_is_unresolved() {
    let keeps =
        "CREATE PROCEDURE p() LANGUAGE plpgsql AS $$\nBEGIN\n    RAISE NOTICE 'hi';\nEND $$;\n";
    let clears = "CREATE PROCEDURE other.p() LANGUAGE plpgsql AS $$\nBEGIN\n    \
                  PERFORM set_config('lock_timeout', '0', true);\nEND $$;\n";
    let sql = format!(
        "{keeps}{clears}SET LOCAL lock_timeout = '5s';\nSET LOCAL search_path = other, public;\n\
         CALL p();\nALTER TABLE harvest_events ADD COLUMN x INT;"
    );
    // The unread call and the ALTER each lack a bound.
    let findings = lint_with_history(&[], &sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout; 2], "{sql}");
}

#[test]
fn nonstandard_strings_make_the_migration_unreadable() {
    for set in [
        "SET standard_conforming_strings = off;",
        "SET LOCAL standard_conforming_strings TO 'false';",
        "SELECT set_config('standard_conforming_strings', 'off', false);",
    ] {
        let sql = format!(
            "SET LOCAL lock_timeout = '5s';\n{set}\nDO 'BEGIN ALTER TABLE harvest_event\\163 ADD COLUMN x INT; END';"
        );
        let findings = lint_with_history(&[], &sql, true);
        assert!(rules(&findings).contains(&Rule::LockTimeout), "{sql}");
        assert!(
            findings
                .iter()
                .any(|f| f.detail.contains("standard_conforming_strings")),
            "{findings:?}"
        );
    }
    // Turning it on is the default and changes nothing.
    let sql = "SET standard_conforming_strings = on;";
    assert_eq!(lint_with_history(&[], sql, true), [], "{sql}");
}

#[test]
fn a_path_change_in_an_earlier_migration_taints_set_config() {
    let earlier = "SET search_path = app, pg_catalog;";
    let lock = "ALTER TABLE harvest_events ADD COLUMN x INT;";
    let sql = format!("SELECT set_config('lock_timeout', '5s', true);\n{lock}");
    let findings = lint_with_history(&[earlier], &sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}");
    let sql = format!("SELECT pg_catalog.set_config('lock_timeout', '5s', true);\n{lock}");
    assert_eq!(lint_with_history(&[earlier], &sql, true), [], "{sql}");
}

#[test]
fn a_carriage_return_ends_a_line_comment() {
    let sql = "-- comment\rALTER TABLE harvest_events ADD COLUMN x INT;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql:?}");
    // A literal continues across a lone carriage return too.
    let sql = "DO $$\rBEGIN\r    EXECUTE 'ALTER TABLE harvest_'\r        'events ADD COLUMN x INT';\rEND $$;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql:?}");
    assert!(
        findings[0].detail.contains("harvest_events"),
        "{findings:?}"
    );
}

#[test]
fn a_quoted_constraint_word_builds_no_index() {
    for action in [
        "ADD CHECK (\"unique\" IS NOT NULL)",
        "ADD CHECK (\"exclude\" > 0)",
        "ADD CHECK (\"primary\" IS NOT NULL AND \"key\" > 0)",
    ] {
        let sql = format!("SET LOCAL lock_timeout = '5s';\nALTER TABLE harvest_events {action};");
        assert_eq!(lint_with_history(&[], &sql, true), [], "{sql}");
    }
}

#[test]
fn an_alter_routine_that_drops_its_bound_is_unbounded() {
    let create = "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql SET lock_timeout = '5s' AS $$\n\
                  BEGIN\n    ALTER TABLE harvest_events ADD COLUMN x INT;\nEND $$;\n";
    for alter in [
        "ALTER FUNCTION f() RESET lock_timeout;",
        "ALTER FUNCTION f() SET lock_timeout = 0;",
        "ALTER ROUTINE f() RESET ALL;",
        "ALTER PROCEDURE f() SET lock_timeout TO DEFAULT;",
    ] {
        let sql = format!("{create}{alter}");
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}");
        // The body may come from an earlier migration.
        let findings = lint_with_history(&[create], alter, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{alter}");
    }
    let sql = format!("{create}ALTER FUNCTION f() SET lock_timeout = '10s';");
    assert_eq!(lint_with_history(&[], &sql, true), [], "{sql}");
}

#[test]
fn nonstandard_strings_carry_into_later_migrations() {
    let off = "-- lock-safety: allow lock-timeout #1810 test fixture\nSET standard_conforming_strings = off;";
    let hidden = "SET LOCAL lock_timeout = '5s';\nDO 'BEGIN ALTER TABLE harvest_event\\163 ADD COLUMN x INT; END';";
    let findings = lint_with_history(&[off], hidden, true);
    assert!(
        findings
            .iter()
            .any(|f| f.detail.contains("standard_conforming_strings")),
        "{findings:?}"
    );
    // Without a backslash, the setting changes nothing.
    let plain = "SET LOCAL lock_timeout = '5s';\nALTER TABLE harvest_events ADD COLUMN x INT;";
    assert_eq!(lint_with_history(&[off], plain, true), []);
    // A later reset ends the taint.
    let reset = format!("{off}\nRESET standard_conforming_strings;");
    let findings = lint_with_history(&[&reset], hidden, true);
    assert!(
        !findings
            .iter()
            .any(|f| f.detail.contains("standard_conforming_strings")),
        "{findings:?}"
    );
}

#[test]
fn only_the_built_in_set_config_turns_conforming_strings_on() {
    let off = "-- lock-safety: allow lock-timeout #1810 test fixture\n\
               SET standard_conforming_strings = off;";
    let hidden = "SET LOCAL lock_timeout = '5s';\nDO 'BEGIN ALTER TABLE harvest_event\\163 ADD COLUMN x INT; END';";
    let tainted = |findings: &[Finding]| {
        findings
            .iter()
            .any(|f| f.detail.contains("standard_conforming_strings"))
    };
    // Another schema's `set_config`, or an unqualified one after a path
    // change, may be a user function.
    for on in [
        "SELECT other.set_config('standard_conforming_strings', 'on', false);",
        "SET search_path = other, pg_catalog;\nSELECT set_config('standard_conforming_strings', 'on', false);",
    ] {
        let sql = format!("{on}\n{hidden}");
        let findings = lint_with_history(&[off], &sql, true);
        assert!(tainted(&findings), "{sql}\n{findings:?}");
    }
    // The built-in turns it on.
    for on in [
        "SELECT pg_catalog.set_config('standard_conforming_strings', 'on', false);",
        "SELECT set_config('standard_conforming_strings', 'on', false);",
    ] {
        let sql = format!("{on}\n{hidden}");
        let findings = lint_with_history(&[off], &sql, true);
        assert!(!tainted(&findings), "{sql}\n{findings:?}");
    }
}

#[test]
fn opaque_code_may_turn_conforming_strings_off() {
    let allow = "-- lock-safety: allow lock-timeout #1810 test fixture\n";
    let hidden = "SET LOCAL lock_timeout = '5s';\nDO 'BEGIN ALTER TABLE harvest_event\\163 ADD COLUMN x INT; END';";
    for opaque in [
        "DO $$\nplpy.execute(\"SET standard_conforming_strings = off\")\n$$ LANGUAGE plpython3u;",
        "CALL mystery();",
    ] {
        let sql = format!("{allow}{opaque}\n{hidden}");
        let findings = lint_with_history(&[], &sql, true);
        assert!(
            findings
                .iter()
                .any(|f| f.detail.contains("standard_conforming_strings")),
            "{sql}\n{findings:?}"
        );
    }
}

#[test]
fn opaque_code_leaves_later_index_names_unplaced() {
    let allow = "-- lock-safety: allow lock-timeout #1810 test fixture\n";
    let create = "CREATE INDEX idx ON scratch_t (x);";
    let drop = "DROP INDEX idx;";
    // The unread call may change `search_path`, so `idx` may sit in another
    // schema, and `idx` stays unknown.
    let history = format!("{allow}CALL mystery();\n{create}");
    let findings = lint_with_history(&[&history], drop, true);
    assert!(!findings.is_empty(), "{findings:?}");
    // The doubt holds for that file only, so a later file places `idx`.
    let history = [format!("{allow}CALL mystery();"), create.to_string()];
    let history: Vec<&str> = history.iter().map(String::as_str).collect();
    assert_eq!(lint_with_history(&history, drop, true), [], "{history:?}");
}

#[test]
fn a_computed_setting_name_may_turn_conforming_strings_off() {
    let hidden = "SET LOCAL lock_timeout = '5s';\nDO 'BEGIN ALTER TABLE harvest_event\\163 ADD COLUMN x INT; END';";
    for call in [
        "SELECT set_config('standard_' || 'conforming_strings', 'off', false);",
        "SELECT set_config(name_var, 'off', false);",
    ] {
        let sql =
            format!("-- lock-safety: allow lock-timeout #1810 test fixture\n{call}\n{hidden}");
        let findings = lint_with_history(&[], &sql, true);
        assert!(
            findings
                .iter()
                .any(|f| f.detail.contains("standard_conforming_strings")),
            "{sql}\n{findings:?}"
        );
    }
    // A plain literal that names another setting changes nothing.
    let sql = "SELECT set_config('application_name', 'off', false);";
    assert_eq!(lint_with_history(&[], sql, true), [], "{sql}");
}

#[test]
fn a_local_nonstandard_setting_ends_at_the_commit() {
    let allow = "-- lock-safety: allow lock-timeout #1810 test fixture\n";
    let hidden = "SET LOCAL lock_timeout = '5s';\nDO 'BEGIN ALTER TABLE harvest_event\\163 ADD COLUMN x INT; END';";
    let tainted = |findings: &[Finding]| {
        findings
            .iter()
            .any(|f| f.detail.contains("standard_conforming_strings"))
    };
    for local in [
        "SET LOCAL standard_conforming_strings = off;",
        "SELECT set_config('standard_conforming_strings', 'off', true);",
    ] {
        // The file ends its transaction, so a later migration reads as `on`.
        let earlier = format!("{allow}{local}");
        let findings = lint_with_history(&[&earlier], hidden, true);
        assert!(!tainted(&findings), "{earlier}\n{findings:?}");
        // A `COMMIT` ends the local value inside the file too.
        let sql = format!("{allow}{local}\nCOMMIT;\n{hidden}");
        let findings = lint_with_history(&[], &sql, false);
        assert!(!tainted(&findings), "{sql}\n{findings:?}");
        // Before the commit, the local value hides the body.
        let sql = format!("{allow}{local}\n{hidden}");
        let findings = lint_with_history(&[], &sql, true);
        assert!(tainted(&findings), "{sql}\n{findings:?}");
    }
    // A routine body may run its local value in any later transaction.
    let earlier = format!(
        "{allow}CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    \
         PERFORM set_config('standard_conforming_strings', 'off', true);\nEND $$;"
    );
    let findings = lint_with_history(&[&earlier], hidden, true);
    assert!(tainted(&findings), "{earlier}\n{findings:?}");
    // A session value under a local one still carries.
    let earlier = format!(
        "{allow}SET standard_conforming_strings = off;\n\
         {allow}SET LOCAL standard_conforming_strings = off;"
    );
    let findings = lint_with_history(&[&earlier], hidden, true);
    assert!(tainted(&findings), "{earlier}\n{findings:?}");
}

#[test]
fn a_conforming_strings_change_that_may_not_run_keeps_the_taint() {
    let off = "-- lock-safety: allow lock-timeout #1810 test fixture\n\
               SET standard_conforming_strings = off;\n";
    let hidden = "SET LOCAL lock_timeout = '5s';\nDO 'BEGIN ALTER TABLE harvest_event\\163 ADD COLUMN x INT; END';";
    // A function body runs later, and a branch may not run.
    for maybe in [
        "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    \
         SET standard_conforming_strings = on;\nEND $$;",
        "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    \
         PERFORM set_config('standard_conforming_strings', 'on', false);\nEND $$;",
        "DO $$\nBEGIN\n    IF random() < 0.5 THEN\n        \
         RESET standard_conforming_strings;\n    END IF;\nEND $$;",
    ] {
        let sql = format!("{off}{maybe}\n{hidden}");
        let findings = lint_with_history(&[], &sql, true);
        assert!(
            findings
                .iter()
                .any(|f| f.detail.contains("standard_conforming_strings")),
            "{sql}\n{findings:?}"
        );
    }
}

#[test]
fn a_foreign_do_body_makes_no_table_new() {
    let sql = "-- lock-safety: allow lock-timeout #1810 test fixture\n\
               DO $$\n# ; CREATE TABLE harvest_events (id BIGINT);\npass\n$$ LANGUAGE plpython3u;\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;";
    let findings = lint_with_history(&[], sql, true);
    assert!(
        findings
            .iter()
            .any(|f| f.detail.starts_with("ALTER TABLE locks harvest_events")),
        "{findings:?}"
    );
}

#[test]
fn a_quoted_object_name_is_not_its_target_keyword() {
    for sql in [
        "CREATE TRIGGER \"on\" BEFORE INSERT ON harvest_events FOR EACH ROW EXECUTE FUNCTION f();",
        "DROP TRIGGER \"on\" ON harvest_events;",
        "ALTER TRIGGER \"on\" ON harvest_events RENAME TO t2;",
        "CREATE RULE \"to\" AS ON INSERT TO harvest_events DO INSTEAD NOTHING;",
    ] {
        let findings = lint_with_history(&[], sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}");
        assert!(
            findings[0].detail.contains("harvest_events"),
            "{findings:?}"
        );
    }
}

#[test]
fn an_annotated_nonstandard_setter_cannot_hide_a_later_body() {
    let sql = "SET LOCAL lock_timeout = '5s';\n\
               -- lock-safety: allow lock-timeout #1810 test fixture\n\
               SET standard_conforming_strings = off;\n\
               DO 'BEGIN ALTER TABLE harvest_event\\163 ADD COLUMN x INT; END';";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
    assert_eq!(findings[0].line, 4, "{findings:?}");
    assert!(
        findings[0].detail.contains("standard_conforming_strings"),
        "{findings:?}"
    );
}

#[test]
fn an_inherited_path_change_leaves_index_names_unplaced() {
    let create = "CREATE INDEX idx ON scratch_t (x);";
    let drop = "DROP INDEX idx;";
    // Without a path change, the create teaches the history that `idx`
    // sits on a cold table.
    assert_eq!(lint_with_history(&[create], drop, true), [], "{drop}");
    // After an inherited path change, `idx` may sit in another schema, so
    // `idx` stays unknown, which counts as hot.
    let history = ["SET search_path = scratch, public;", create];
    let findings = lint_with_history(&history, drop, true);
    assert!(!findings.is_empty(), "{findings:?}");
}

#[test]
fn a_call_or_unreadable_code_ends_a_new_table() {
    let procedure =
        "CREATE PROCEDURE p() LANGUAGE plpgsql AS $$\nBEGIN\n    RAISE NOTICE 'hi';\nEND $$;\n";
    let function = "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    RAISE NOTICE 'hi';\nEND $$;\n";
    let create = "CREATE TABLE harvest_events (id BIGINT);\n";
    let lock = "ALTER TABLE harvest_events ADD COLUMN x INT;";
    for (before, call) in [
        (procedure, "CALL p();"),
        (function, "SELECT f();"),
        (
            "",
            "DO $$\nDECLARE v text := 'x';\nBEGIN\n    EXECUTE v;\nEND $$;",
        ),
    ] {
        let sql = format!("{before}{create}{call}\n{lock}");
        let findings = lint_with_history(&[], &sql, true);
        assert!(
            findings
                .iter()
                .any(|f| f.detail.contains("ALTER TABLE locks harvest_events")),
            "{sql}\n{findings:?}"
        );
    }
}

#[test]
fn a_call_of_an_earlier_routine_ends_a_new_table() {
    let locks = "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    \
                 DROP TABLE scratch.harvest_events;\nEND $$;";
    let clears = "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    \
                  PERFORM set_config('lock_timeout', '0', true);\nEND $$;";
    let foreign = "CREATE FUNCTION f() RETURNS void LANGUAGE plpython3u AS $$\npass\n$$;";
    let sql = "CREATE TABLE harvest_events (id BIGINT);\nSELECT f();\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;";
    // A routine from an earlier migration may drop or rename the new table.
    for history in [locks, clears, foreign] {
        let findings = lint_with_history(&[history], sql, true);
        assert!(
            findings
                .iter()
                .any(|f| f.detail.contains("ALTER TABLE locks harvest_events")),
            "{history}\n{findings:?}"
        );
    }
}

#[test]
fn a_return_body_runs_only_when_called() {
    let sql = "CREATE FUNCTION clear_timeout() RETURNS text LANGUAGE SQL \
               RETURN set_config('lock_timeout', '0', true);\n\
               SET LOCAL lock_timeout = '5s';\nSELECT clear_timeout();\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;";
    let findings = lint_with_history(&[], sql, true);
    assert!(
        findings
            .iter()
            .any(|f| f.detail.starts_with("ALTER TABLE locks harvest_events")),
        "{findings:?}"
    );
}

#[test]
fn an_inherited_call_needs_the_full_identity_to_keep_the_bound() {
    let history = [
        "CREATE PROCEDURE other.p() LANGUAGE plpgsql AS $$\nBEGIN\n    \
                    ALTER TABLE harvest_events ADD COLUMN y INT;\nEND $$;",
    ];
    // Another schema or another arity may reach a routine that clears.
    for call in ["CALL public.p();", "CALL p();", "CALL other.p(1);"] {
        let sql = format!("SET LOCAL lock_timeout = '5s';\n{call}");
        let findings = lint_with_history(&history, &sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}\n{findings:?}");
    }
    // The same name and arity reach the known body, which does not clear.
    let sql = "SET LOCAL lock_timeout = '5s';\nCALL other.p();";
    assert_eq!(lint_with_history(&history, sql, true), [], "{sql}");
}

#[test]
fn a_call_of_a_self_bounded_routine_needs_no_outside_bound() {
    let lock = "ALTER TABLE harvest_events ADD COLUMN y INT;";
    let proc = |clause: &str, body: &str| {
        format!(
            "CREATE OR REPLACE PROCEDURE p() LANGUAGE plpgsql {clause}AS $$\nBEGIN\n{body}\nEND $$;"
        )
    };
    let setter = "    SET LOCAL lock_timeout = '5s';\n";
    let clause = proc("SET lock_timeout = '5s' ", lock);
    let body = proc("", &format!("{setter}    {lock}"));
    // Postgres applies the routine's own bound on each call.
    for earlier in [&clause, &body] {
        assert_eq!(
            lint_with_history(&[earlier], "CALL p();", true),
            [],
            "{earlier}"
        );
    }
    let function = format!(
        "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql SET lock_timeout = '5s' AS $$\nBEGIN\n    {lock}\nEND $$;"
    );
    assert_eq!(lint_with_history(&[&function], "SELECT f();", true), []);
    // A routine that only calls a self-bounded routine is self-bounded too.
    let caller = "CREATE PROCEDURE q() LANGUAGE plpgsql AS $$\nBEGIN\n    CALL p();\nEND $$;";
    let earlier = format!("{clause}\n{caller}");
    assert_eq!(
        lint_with_history(&[&earlier], "CALL q();", true),
        [],
        "{earlier}"
    );

    let late = proc("", &format!("    {lock}\n{setter}"));
    let branch = proc(
        "",
        &format!("    IF now() > '2000-01-01' THEN\n    {setter}    END IF;\n    {lock}"),
    );
    let allowed = proc(
        "",
        &format!("    -- lock-safety: allow lock-timeout #1810 test fixture\n    {lock}"),
    );
    let nested = format!(
        "CREATE FUNCTION g() RETURNS void LANGUAGE plpgsql AS $o$\nBEGIN\n    {}\nEND $o$;",
        proc("", &format!("    {lock}")).replace("$$", "$i$")
    );
    let opaque = "DO $$\nBEGIN\n    EXECUTE format('%s', 'x');\nEND $$;".to_string();
    let reset = "ALTER PROCEDURE p() RESET lock_timeout;".to_string();
    let unbounded_chain = format!("{allowed}\n{caller}");
    let cases: Vec<(Vec<&str>, &str)> = vec![
        (vec![&late], "CALL p();"),
        (vec![&branch], "CALL p();"),
        (vec![&allowed], "CALL p();"),
        (vec![&clause], "CALL other.p();"),
        (vec![&clause], "CALL p(1);"),
        (vec![&clause, &late], "CALL p();"),
        (vec![&clause, &nested], "CALL p();"),
        (vec![&nested, &clause], "CALL p();"),
        (vec![&clause, &opaque], "CALL p();"),
        (vec![&clause, &reset], "CALL p();"),
        (vec![&clause], "SET search_path = other;\nCALL p();"),
        (vec![&unbounded_chain], "CALL q();"),
    ];
    // Each of these may reach a lock that no bound covers.
    for (history, sql) in cases {
        let findings = lint_with_history(&history, sql, true);
        assert!(
            rules(&findings).contains(&Rule::LockTimeout),
            "{history:?}\n{sql}\n{findings:?}"
        );
    }
    // Unreadable code earlier in the same file may replace the routine too.
    let qualified = clause.replace("PROCEDURE p()", "PROCEDURE app.p()");
    assert_eq!(lint_with_history(&[&qualified], "CALL app.p();", true), []);
    let sql = format!("{opaque}\nCALL app.p();");
    let findings = lint_with_history(&[&qualified], &sql, true);
    let calls = findings.iter().filter(|f| f.line == 5).count();
    assert_eq!(calls, 1, "{sql}\n{findings:?}");
}

#[test]
fn a_routine_created_in_an_uncalled_body_is_not_known() {
    let nested = |clause: &str, body: &str| {
        format!(
            "CREATE FUNCTION outer_f() RETURNS void LANGUAGE plpgsql AS $o$\nBEGIN\n    \
             CREATE OR REPLACE PROCEDURE p() LANGUAGE plpgsql {clause}AS $i$\n    BEGIN\n        \
             {body}\n    END $i$;\nEND $o$;"
        )
    };
    let lock = "ALTER TABLE harvest_events ADD COLUMN y INT;";
    // `p` exists only once `outer_f` runs. Until then, a call may reach
    // another routine of that name, which may clear the bound and lock.
    let bounded = nested("SET lock_timeout = '5s' ", lock);
    let plain = nested("", lock);
    for (history, sql) in [
        (vec![bounded.as_str()], "CALL p();"),
        (
            vec![plain.as_str()],
            "SET LOCAL lock_timeout = '5s';\nCALL p();",
        ),
    ] {
        let findings = lint_with_history(&history, sql, true);
        assert_eq!(
            rules(&findings),
            [Rule::LockTimeout],
            "{history:?}\n{sql}\n{findings:?}"
        );
    }
    // The same holds for a call in the same file.
    let sql = format!(
        "{}\nSET LOCAL lock_timeout = '5s';\nCALL p();",
        nested("", "NULL;")
    );
    let findings = lint_with_history(&[], &sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}\n{findings:?}");
    // A call of a function that may not exist runs unknown code, which may
    // change `search_path`. So the index is not learnt.
    let function = "CREATE FUNCTION outer_f() RETURNS void LANGUAGE plpgsql AS $o$\nBEGIN\n    \
                    CREATE FUNCTION q() RETURNS void LANGUAGE plpgsql AS $i$\n    BEGIN\n        \
                    NULL;\n    END $i$;\nEND $o$;\n";
    let index = "CREATE INDEX idx ON scratch_t (x);";
    let drop = "DROP INDEX idx;";
    assert_eq!(
        lint_with_history(&[&format!("{function}{index}")], drop, true),
        []
    );
    let history = format!("{function}SELECT q();\n{index}");
    let findings = lint_with_history(&[&history], drop, true);
    assert!(!findings.is_empty(), "{history}\n{findings:?}");
}

#[test]
fn an_uncertain_definition_does_not_keep_the_bound() {
    // `p` may not exist, so the call may reach a routine that clears the bound.
    let nested = "CREATE FUNCTION outer_f() RETURNS void LANGUAGE plpgsql AS $o$\nBEGIN\n    \
                  CREATE OR REPLACE PROCEDURE p() LANGUAGE plpgsql AS $i$\n    BEGIN\n        \
                  NULL;\n    END $i$;\nEND $o$;";
    let sql = format!(
        "SET LOCAL lock_timeout = '5s';\n{nested}\n\
         -- lock-safety: allow lock-timeout #1810 test fixture\nCALL p();\n\
         ALTER TABLE harvest_events ADD COLUMN x INT;"
    );
    let findings = lint_with_history(&[], &sql, true);
    assert!(
        findings
            .iter()
            .any(|f| f.detail.starts_with("ALTER TABLE locks harvest_events")),
        "{sql}\n{findings:?}"
    );
}

#[test]
fn a_rolled_back_definition_is_not_known() {
    let lock = "ALTER TABLE harvest_events ADD COLUMN y INT;";
    let create = |clause: &str, body: &str| {
        format!(
            "CREATE OR REPLACE PROCEDURE p() LANGUAGE plpgsql {clause}AS $$\nBEGIN\n    {body}\nEND $$;\n\
             ROLLBACK;"
        )
    };
    // The `ROLLBACK` undoes the `CREATE`, so a call reaches the old `p`.
    let bounded = create("SET lock_timeout = '5s' ", lock);
    let plain = create("", lock);
    for (history, sql) in [
        (vec![bounded.as_str()], "CALL p();"),
        (
            vec![plain.as_str()],
            "SET LOCAL lock_timeout = '5s';\nCALL p();",
        ),
    ] {
        let findings = lint_with_history(&history, sql, false);
        assert_eq!(
            rules(&findings),
            [Rule::LockTimeout],
            "{history:?}\n{sql}\n{findings:?}"
        );
    }
    let sql = format!(
        "{}\nSET LOCAL lock_timeout = '5s';\nCALL p();",
        create("", "NULL;")
    );
    let findings = lint_with_history(&[], &sql, false);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}\n{findings:?}");
}

#[test]
fn a_routine_search_path_unplaces_its_unqualified_calls() {
    let defs = |schema: &str| {
        format!(
            "CREATE PROCEDURE {schema}p() LANGUAGE plpgsql AS $$\nBEGIN\n    NULL;\nEND $$;\n\
             CREATE PROCEDURE w() LANGUAGE plpgsql SET search_path = other, public \
             SET lock_timeout = '5s' AS $$\nBEGIN\n    CALL {schema}p();\n    \
             ALTER TABLE harvest_events ADD COLUMN y INT;\nEND $$;"
        )
    };
    // `other.p` may clear the bound before `w` locks.
    let unqualified = defs("");
    let findings = lint_with_history(&[], &unqualified, true);
    assert!(
        rules(&findings).contains(&Rule::LockTimeout),
        "{findings:?}"
    );
    let findings = lint_with_history(&[&unqualified], "CALL w();", true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
    // A call with a schema reaches the known routine.
    let qualified = defs("public.");
    assert_eq!(lint_with_history(&[], &qualified, true), []);
    assert_eq!(lint_with_history(&[&qualified], "CALL w();", true), []);
}

#[test]
fn an_in_file_overload_does_not_hide_an_inherited_locking_one() {
    let earlier = "CREATE FUNCTION f(a int) RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    \
                   ALTER TABLE harvest_events ADD COLUMN y INT;\nEND $$;";
    // The lint does not compare types, so `f(1)` may reach the locking `f(int)`.
    let sql = "CREATE FUNCTION f(a text) RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    \
               NULL;\nEND $$;\nSELECT f(1);";
    let findings = lint_with_history(&[earlier], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn an_array_comma_is_not_an_argument_separator() {
    let routine = |params: &str| {
        format!(
            "CREATE FUNCTION f({params}) RETURNS void LANGUAGE plpgsql SET lock_timeout = '5s' \
             AS $$\nBEGIN\n    ALTER TABLE harvest_events ADD COLUMN y INT;\nEND $$;"
        )
    };
    // Each call reaches the self-bounded `f`, so it needs no outside bound.
    for (params, call) in [
        ("a int[]", "SELECT f(ARRAY[1,2]);"),
        ("a int[] DEFAULT ARRAY[1,2]", "SELECT f();"),
        ("a int[] DEFAULT ARRAY[1,2]", "SELECT f(ARRAY[3,4]);"),
    ] {
        let earlier = routine(params);
        let findings = lint_with_history(&[&earlier], call, true);
        assert_eq!(findings, [], "{earlier}\n{call}");
    }
}

#[test]
fn an_inner_routine_bound_does_not_cover_the_outer_body() {
    // Creating the inner routine changes nothing for the outer call.
    for (clause, setter) in [
        ("SET lock_timeout = '5s' ", ""),
        ("", "        SET LOCAL lock_timeout = '5s';\n"),
    ] {
        let sql = format!(
            "CREATE FUNCTION outer_f() RETURNS void LANGUAGE plpgsql AS $o$\nBEGIN\n    \
             CREATE FUNCTION inner_f() RETURNS void LANGUAGE plpgsql {clause}AS $i$\n    BEGIN\n\
             {setter}        NULL;\n    END $i$;\n    \
             ALTER TABLE harvest_events ADD COLUMN x INT;\nEND $o$;"
        );
        let findings = lint_with_history(&[], &sql, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}\n{findings:?}");
    }
}

#[test]
fn an_altered_routine_keeps_its_history() {
    let create = "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    \
                  ALTER TABLE harvest_events ADD COLUMN y INT;\nEND $$;";
    let set = "SET LOCAL lock_timeout = '5s';\n";
    // A known routine keeps the outer bound.
    let sql = format!("{set}SELECT f();");
    assert_eq!(lint_with_history(&[create], &sql, true), [], "{sql}");
    // A zero setting clears the bound before the body locks. A rename or a
    // schema move gives the routine a name the bound does not know.
    for (alter, call) in [
        ("ALTER FUNCTION f() SET lock_timeout = '0';", "SELECT f();"),
        ("ALTER FUNCTION f() RENAME TO g;", "SELECT g();"),
        ("ALTER FUNCTION f() SET SCHEMA other;", "SELECT other.f();"),
        ("ALTER FUNCTION f() SET SCHEMA other;", "SELECT f();"),
    ] {
        let sql = format!("{set}{call}");
        let findings = lint_with_history(&[create, alter], &sql, true);
        assert_eq!(
            rules(&findings),
            [Rule::LockTimeout],
            "{alter} {call}\n{findings:?}"
        );
    }
}

#[test]
fn a_conditional_commit_does_not_save_the_bound() {
    // The `COMMIT` may not run, so the `ROLLBACK` may restore the value from
    // before the `SET`.
    let sql = "SET lock_timeout = '5s';\nDO $$\nBEGIN\n    IF random() < 0.5 THEN\n        COMMIT;\n    \
               END IF;\nEND $$;\nROLLBACK;\nALTER TABLE harvest_events ADD COLUMN x INT;";
    let findings = lint_with_history(&[], sql, false);
    assert!(
        findings
            .iter()
            .any(|f| f.detail.starts_with("ALTER TABLE locks harvest_events")),
        "{findings:?}"
    );
}

#[test]
fn a_conditional_commit_keeps_a_bound_only_on_both_paths() {
    let branch =
        "DO $$\nBEGIN\n    IF random() < 0.5 THEN\n        COMMIT;\n    END IF;\nEND $$;\n";
    let lock = "ALTER TABLE harvest_events ADD COLUMN x INT;";
    // Without the commit, the local value holds. With it, the session value holds.
    for (session, local, bounded) in [("5s", "0", false), ("0", "5s", false), ("5s", "3s", true)] {
        let sql = format!(
            "SET lock_timeout = '{session}';\nSET LOCAL lock_timeout = '{local}';\n{branch}{lock}"
        );
        let findings = lint_with_history(&[], &sql, false);
        let flagged = findings
            .iter()
            .any(|f| f.detail.starts_with("ALTER TABLE locks harvest_events"));
        assert_eq!(flagged, !bounded, "{sql}\n{findings:?}");
    }
}

#[test]
fn a_local_path_change_does_not_carry_into_later_migrations() {
    let create = "CREATE INDEX idx ON scratch_t (x);";
    let drop = "DROP INDEX idx;";
    for local in [
        "SET LOCAL search_path = scratch, public;",
        "SELECT set_config('search_path', 'scratch, public', true);",
    ] {
        assert_eq!(
            lint_with_history(&[local, create], drop, true),
            [],
            "{local}"
        );
    }
}

#[test]
fn a_foreign_do_body_takes_no_lock_of_its_own() {
    // The annotated `DO` finding covers the body, which is not SQL.
    let sql = "-- lock-safety: allow lock-timeout #1810 test fixture\n\
               DO $$\n# ; ALTER TABLE harvest_events ADD COLUMN x INT\npass\n$$ LANGUAGE plpython3u;";
    assert_eq!(lint_with_history(&[], sql, true), [], "{sql}");
}

#[test]
fn an_execute_inside_a_control_form_is_dynamic_sql() {
    let history = [
        "CREATE FUNCTION legacy_f() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    \
                    ALTER TABLE harvest_events ADD COLUMN y INT;\nEND $$;",
    ];
    for body in [
        "DO $$\nDECLARE r record;\nBEGIN\n    FOR r IN EXECUTE 'SELECT legacy_f()' LOOP\n        NULL;\n    \
         END LOOP;\nEND $$;",
        "DO $$\nDECLARE r record;\nDECLARE q text := 'x';\nBEGIN\n    FOR r IN EXECUTE q LOOP\n        \
         NULL;\n    END LOOP;\nEND $$;",
        "CREATE FUNCTION g() RETURNS SETOF record LANGUAGE plpgsql AS $$\nBEGIN\n    \
         RETURN QUERY EXECUTE 'SELECT legacy_f()';\nEND $$;",
    ] {
        let findings = lint_with_history(&history, body, true);
        assert_eq!(
            rules(&findings),
            [Rule::LockTimeout],
            "{body}\n{findings:?}"
        );
    }
    // A constant query that locks nothing stays readable.
    let sql = "DO $$\nDECLARE r record;\nBEGIN\n    FOR r IN EXECUTE 'SELECT 1' LOOP\n        NULL;\n    \
               END LOOP;\nEND $$;";
    assert_eq!(lint_with_history(&history, sql, true), [], "{sql}");
}

#[test]
fn a_quoted_full_is_a_vacuum_table() {
    // `"full"` names a cold table, so this is a plain vacuum.
    let sql = "VACUUM \"full\";";
    assert_eq!(lint_with_history(&[], sql, false), [], "{sql}");
}

#[test]
fn a_path_change_in_an_uncalled_body_does_not_carry() {
    let create = "CREATE INDEX idx ON scratch_t (x);";
    let drop = "DROP INDEX idx;";
    for change in [
        "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    \
         SET search_path = scratch, public;\nEND $$;",
        "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    \
         PERFORM set_config('search_path', 'scratch, public', false);\nEND $$;",
        "DO $$\nBEGIN\n    PERFORM set_config('search_path', 'scratch, public', true);\nEND $$;",
    ] {
        assert_eq!(
            lint_with_history(&[change, create], drop, true),
            [],
            "{change}"
        );
    }
}

#[test]
fn a_routine_search_path_may_shadow_set_config() {
    let body = |call: &str| {
        format!(
            "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql SET search_path = public, pg_catalog \
             AS $$\nBEGIN\n    PERFORM {call}('lock_timeout', '5s', true);\n    \
             ALTER TABLE harvest_events ADD COLUMN x INT;\nEND $$;"
        )
    };
    // `public.set_config` may shadow the built-in in that body.
    let sql = body("set_config");
    let findings = lint_with_history(&[], &sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
    let sql = body("pg_catalog.set_config");
    assert_eq!(lint_with_history(&[], &sql, true), [], "{sql}");
}

#[test]
fn an_execute_column_is_not_dynamic_sql() {
    // `EXECUTE` is a non-reserved word, so it may name a column or a variable.
    for statement in [
        "PERFORM execute FROM cold_table;",
        "SELECT execute INTO v FROM cold_table;",
        "execute := 1;",
    ] {
        let sql = format!("DO $$\nDECLARE v int;\nBEGIN\n    {statement}\nEND $$;");
        assert_eq!(lint_with_history(&[], &sql, true), [], "{sql}");
    }
}

#[test]
fn return_next_and_return_query_do_not_exit() {
    for ret in ["RETURN NEXT 1;", "RETURN QUERY SELECT 1;"] {
        let sql = format!(
            "CREATE FUNCTION f() RETURNS SETOF int LANGUAGE plpgsql AS $$\nBEGIN\n    {ret}\n    \
             SET LOCAL lock_timeout = '5s';\n    ALTER TABLE harvest_events ADD COLUMN x INT;\nEND $$;"
        );
        assert_eq!(lint_with_history(&[], &sql, true), [], "{sql}");
    }
    // A bare `RETURN` exits, so what follows may never run.
    let sql = "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    RETURN;\n    \
               SET LOCAL lock_timeout = '5s';\n    ALTER TABLE harvest_events ADD COLUMN x INT;\nEND $$;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn an_uncalled_body_does_not_change_the_path_of_its_file() {
    let routine = "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    \
                   SET search_path = scratch, public;\nEND $$;\n";
    let index = "CREATE INDEX idx ON scratch_t (x);";
    let history = format!("{routine}{index}");
    let drop = "DROP INDEX idx;";
    assert_eq!(lint_with_history(&[&history], drop, true), [], "{history}");
    // A call of the routine in the same file runs the change, and so does a
    // call of a routine that calls it.
    let outer = "CREATE FUNCTION g() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    \
                 PERFORM f();\nEND $$;\n";
    for call in ["SELECT f();\n", "SELECT g();\n"] {
        let history = format!("{routine}{outer}{call}{index}");
        let findings = lint_with_history(&[&history], drop, true);
        assert!(!findings.is_empty(), "{history}\n{findings:?}");
    }
}

#[test]
fn a_path_routine_from_an_earlier_migration_changes_the_path() {
    let routine = "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    \
                   PERFORM set_config('search_path', 'scratch, public', false);\nEND $$;";
    let outer = "CREATE FUNCTION g() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    \
                 PERFORM f();\nEND $$;";
    let rename = "ALTER FUNCTION f() RENAME TO h;";
    let index = "CREATE INDEX idx ON scratch_t (x);";
    let drop = "DROP INDEX idx;";
    assert_eq!(lint_with_history(&[index], drop, true), []);
    // The call runs the change, so the index is not learnt.
    for (earlier, call) in [
        (vec![routine], "SELECT f();"),
        (vec![routine, outer], "SELECT g();"),
        (vec![routine, rename], "SELECT h();"),
    ] {
        let migration = format!("{call}\n{index}");
        let history: Vec<&str> = earlier
            .iter()
            .copied()
            .chain([migration.as_str()])
            .collect();
        let findings = lint_with_history(&history, drop, true);
        assert!(!findings.is_empty(), "{history:?}\n{findings:?}");
    }
}

#[test]
fn a_role_change_may_change_the_path() {
    let index = "CREATE INDEX idx ON scratch_t (x);";
    let drop = "DROP INDEX idx;";
    assert_eq!(lint_with_history(&[index], drop, true), []);
    // The `"$user"` entry of `search_path` follows the current role.
    for change in [
        "SET ROLE app_owner;",
        "SET LOCAL ROLE app_owner;",
        "SET role = app_owner;",
        "SET SESSION AUTHORIZATION app_owner;",
        "RESET ROLE;",
        "RESET SESSION AUTHORIZATION;",
        "SELECT set_config('role', 'app_owner', false);",
    ] {
        let history = format!("{change}\n{index}");
        let findings = lint_with_history(&[&history], drop, true);
        assert!(!findings.is_empty(), "{history}\n{findings:?}");
    }
    // A session role change carries into a later migration.
    let findings = lint_with_history(&["SET ROLE app_owner;", index], drop, true);
    assert!(!findings.is_empty(), "{findings:?}");
}

#[test]
fn a_routine_that_runs_opaque_code_may_change_the_path() {
    let index = "CREATE INDEX idx ON scratch_t (x);";
    let drop = "DROP INDEX idx;";
    for body in ["CALL mystery();", "EXECUTE v;"] {
        let routine = format!(
            "CREATE FUNCTION outer_f(v text) RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    \
             {body}\nEND $$;\n"
        );
        assert_eq!(
            lint_with_history(&[&format!("{routine}{index}")], drop, true),
            []
        );
        let history = format!("{routine}SELECT outer_f('x');\n{index}");
        let findings = lint_with_history(&[&history], drop, true);
        assert!(!findings.is_empty(), "{history}\n{findings:?}");
    }
}

#[test]
fn an_unqualified_build_is_not_placed_in_public() {
    // The connection may start with another `search_path`, so the build may
    // land in another schema. `public.idx` may be an unknown hot index.
    let create = "CREATE INDEX idx ON scratch_t (x);";
    let findings = lint_with_history(&[create], "DROP INDEX public.idx;", true);
    assert!(!findings.is_empty(), "{findings:?}");
    // An unqualified drop resolves on the same path as the build.
    assert_eq!(lint_with_history(&[create], "DROP INDEX idx;", true), []);
    // A build on a qualified table has a known schema.
    let qualified = "CREATE INDEX idx ON public.scratch_t (x);";
    assert_eq!(
        lint_with_history(&[qualified], "DROP INDEX public.idx;", true),
        []
    );
}

#[test]
fn an_atomic_body_setter_covers_a_later_call() {
    let history = [
        "CREATE FUNCTION legacy_f() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    \
                    ALTER TABLE harvest_events ADD COLUMN y INT;\nEND $$;",
    ];
    let sql = "CREATE FUNCTION g() RETURNS void LANGUAGE sql\nBEGIN ATOMIC\n    \
               SELECT set_config('lock_timeout', '5s', true);\n    SELECT legacy_f();\nEND;";
    assert_eq!(lint_with_history(&history, sql, true), [], "{sql}");
    let sql = "CREATE FUNCTION g() RETURNS void LANGUAGE sql\nBEGIN ATOMIC\n    \
               SELECT 1;\n    SELECT legacy_f();\nEND;";
    let findings = lint_with_history(&history, sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn a_call_may_omit_defaulted_parameters() {
    let history = [
        "CREATE FUNCTION f(a int, b int DEFAULT 1) RETURNS void LANGUAGE plpgsql AS $$\n\
                    BEGIN\n    ALTER TABLE harvest_events ADD COLUMN y INT;\nEND $$;",
    ];
    for call in ["SELECT f(1);", "SELECT f(1, 2);"] {
        let sql = format!("SET LOCAL lock_timeout = '5s';\n{call}");
        assert_eq!(lint_with_history(&history, &sql, true), [], "{sql}");
    }
    // Too few arguments may reach another routine.
    let sql = "SET LOCAL lock_timeout = '5s';\nSELECT f();";
    let findings = lint_with_history(&history, sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
    // In the same file, the call reaches the known body.
    let sql = "CREATE PROCEDURE p(a int, b int = 1) LANGUAGE plpgsql AS $$\nBEGIN\n    \
               RAISE NOTICE 'hi';\nEND $$;\nSET LOCAL lock_timeout = '5s';\nCALL p(1);\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;";
    assert_eq!(lint_with_history(&[], sql, true), [], "{sql}");
}

#[test]
fn an_inner_routine_lock_is_not_the_outer_routine_lock() {
    for setter in ["", "        SET LOCAL lock_timeout = '5s';\n"] {
        let history = [format!(
            "CREATE FUNCTION outer_f() RETURNS void LANGUAGE plpgsql AS $o$\nBEGIN\n    \
             CREATE OR REPLACE FUNCTION inner_f() RETURNS void LANGUAGE plpgsql AS $i$\n    \
             BEGIN\n{setter}        ALTER TABLE harvest_events ADD COLUMN y INT;\n    END $i$;\nEND $o$;"
        )];
        let history = [history[0].as_str()];
        // Calling `outer_f` only creates `inner_f`, so it takes no lock.
        assert_eq!(lint_with_history(&history, "SELECT outer_f();", true), []);
        // A call of `inner_f` counts as a lock. `inner_f` may not exist, so
        // the call may reach another routine of that name.
        let findings = lint_with_history(&history, "SELECT inner_f();", true);
        assert_eq!(
            rules(&findings),
            [Rule::LockTimeout],
            "{setter}\n{findings:?}"
        );
    }
}

#[test]
fn out_parameters_of_a_function_are_not_arguments() {
    let history = [
        "CREATE FUNCTION f(a int, OUT result int) LANGUAGE plpgsql AS $$\nBEGIN\n    \
                    ALTER TABLE harvest_events ADD COLUMN y INT;\n    result := 1;\nEND $$;",
    ];
    let set = "SET LOCAL lock_timeout = '5s';\n";
    let sql = format!("{set}SELECT f(1);");
    assert_eq!(lint_with_history(&history, &sql, true), [], "{sql}");
    // Two arguments reach another routine.
    let sql = format!("{set}SELECT f(1, 2);");
    let findings = lint_with_history(&history, &sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
    // A `CALL` passes the `OUT` parameters of a procedure.
    let history = [
        "CREATE PROCEDURE p(a int, OUT result int) LANGUAGE plpgsql AS $$\nBEGIN\n    \
                    ALTER TABLE harvest_events ADD COLUMN y INT;\n    result := 1;\nEND $$;",
    ];
    let sql = format!("{set}CALL p(1, NULL);");
    assert_eq!(lint_with_history(&history, &sql, true), [], "{sql}");
}

#[test]
fn an_inner_routine_clear_is_not_the_outer_routine_clear() {
    // Calling `outer_f` only creates `inner_f`, so the bound holds.
    let sql = "CREATE FUNCTION outer_f() RETURNS void LANGUAGE plpgsql AS $o$\nBEGIN\n    \
               CREATE OR REPLACE FUNCTION inner_f() RETURNS void LANGUAGE plpgsql AS $i$\n    \
               BEGIN\n        RESET lock_timeout;\n    END $i$;\nEND $o$;\n\
               SET LOCAL lock_timeout = '5s';\nSELECT outer_f();\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;";
    assert_eq!(lint_with_history(&[], sql, true), [], "{sql}");
    // An atomic body that clears still clears.
    let sql = "CREATE FUNCTION g() RETURNS void LANGUAGE sql\nBEGIN ATOMIC\n    SELECT 1;\n    \
               SELECT set_config('lock_timeout', '0', false);\nEND;\n\
               SET LOCAL lock_timeout = '5s';\nSELECT g();\n\
               ALTER TABLE harvest_events ADD COLUMN x INT;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{findings:?}");
}

#[test]
fn a_call_of_an_unread_or_locking_routine_is_a_lock() {
    let procedure = "CREATE PROCEDURE legacy_p() LANGUAGE plpgsql AS $$\nBEGIN\n    \
                     ALTER TABLE harvest_events ADD COLUMN y INT;\nEND $$;";
    let function = "CREATE FUNCTION legacy_f() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n    \
                    ALTER TABLE harvest_events ADD COLUMN y INT;\nEND $$;";
    for (history, call) in [
        (vec![procedure], "CALL legacy_p();"),
        (vec![function], "SELECT legacy_f();"),
    ] {
        let findings = lint_with_history(&history, call, true);
        assert_eq!(rules(&findings), [Rule::LockTimeout], "{call}");
        // A bound before the call covers the lock.
        let sql = format!("SET LOCAL lock_timeout = '5s';\n{call}");
        assert_eq!(lint_with_history(&history, &sql, true), [], "{sql}");
    }
    // An unknown routine may clear the bound before it locks, so no bound
    // covers the call.
    let sql = "SET LOCAL lock_timeout = '5s';\nCALL mystery();";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}");
}

#[test]
fn a_local_conforming_strings_value_keeps_the_session_taint() {
    let off = "-- lock-safety: allow lock-timeout #1810 test fixture\nSET standard_conforming_strings = off;\n";
    let hidden = "SET LOCAL lock_timeout = '5s';\nDO 'BEGIN ALTER TABLE harvest_event\\163 ADD COLUMN x INT; END';";
    for local in [
        "SET LOCAL standard_conforming_strings = on;",
        "SELECT set_config('standard_conforming_strings', 'on', true);",
    ] {
        let earlier = format!("{off}{local}");
        let findings = lint_with_history(&[&earlier], hidden, true);
        assert!(
            findings
                .iter()
                .any(|f| f.detail.contains("standard_conforming_strings")),
            "{local}"
        );
    }
    let earlier = format!("{off}SELECT set_config('standard_conforming_strings', 'on', false);");
    let findings = lint_with_history(&[&earlier], hidden, true);
    assert!(
        !findings
            .iter()
            .any(|f| f.detail.contains("standard_conforming_strings")),
        "{findings:?}"
    );
}

#[test]
fn a_quoted_end_does_not_close_an_atomic_body() {
    let sql = "SET LOCAL lock_timeout = '5s';\n\
               CREATE FUNCTION f() RETURNS void LANGUAGE sql BEGIN ATOMIC\n    \
               SELECT \"end\" FROM t;\n    ALTER TABLE harvest_events ADD COLUMN x INT;\nEND;";
    let findings = lint_with_history(&[], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}");
}

#[test]
fn a_call_of_a_routine_that_clears_and_locks_is_unbounded() {
    let legacy = "CREATE PROCEDURE p() LANGUAGE plpgsql AS $$\nBEGIN\n    \
                  PERFORM set_config('lock_timeout', '0', false);\n    \
                  ALTER TABLE harvest_events ADD COLUMN y INT;\nEND $$;";
    let sql = "SET LOCAL lock_timeout = '5s';\nCALL p();";
    let findings = lint_with_history(&[legacy], sql, true);
    assert_eq!(rules(&findings), [Rule::LockTimeout], "{sql}");
}

#[test]
fn a_rollback_restores_the_conforming_strings_value() {
    let off = "-- lock-safety: allow lock-timeout #1810 test fixture\nSET standard_conforming_strings = off;";
    let sql = "SET standard_conforming_strings = on;\nROLLBACK;\nSET LOCAL lock_timeout = '5s';\n\
               DO 'BEGIN ALTER TABLE harvest_event\\163 ADD COLUMN x INT; END';";
    let findings = lint_with_history(&[off], sql, false);
    assert!(
        findings
            .iter()
            .any(|f| f.detail.contains("standard_conforming_strings")),
        "{findings:?}"
    );
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
    let all = lint_all(&migrations);
    let at = migrations
        .iter()
        .position(|m| m.name == NAME)
        .expect("the migration issue #1810 names is on disk");
    assert!(
        migrations[at].in_scope(),
        "{NAME} must stay inside the cutoff"
    );

    let findings = &all[at];
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
    assert!(rules(findings).contains(&Rule::LockTimeout), "{findings:?}");

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
    let all = lint_all(&migrations);
    let mut failures = Vec::new();
    for (m, findings) in migrations.iter().zip(all).filter(|(m, _)| m.in_scope()) {
        for f in findings {
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
fn legacy_entries_are_on_disk_and_before_the_cutoff() {
    let migrations = load_migrations();
    let on_disk: BTreeSet<String> = migrations
        .iter()
        .map(|m| format!("{}/{}", m.tree, m.name))
        .collect();
    let legacy = legacy_migrations();
    for entry in &legacy {
        let version = entry.rsplit('/').next().and_then(|n| n.split('_').next());
        assert!(on_disk.contains(*entry), "{entry} is not on disk");
        assert!(
            version.is_some_and(|v| v <= LOCK_SAFETY_CUTOFF),
            "{entry} is after the cutoff"
        );
    }
}

#[test]
fn the_legacy_list_is_frozen() {
    let mut a = legacy_migrations();
    // Swapping one entry for another keeps the size but changes the digest.
    let first = *a.iter().next().expect("the list is not empty");
    a.remove(first);
    a.insert("autumn-harvest/migrations/20260914010101_new_change");
    assert_eq!(a.len(), legacy_migrations().len());
    assert_ne!(digest(&a), digest(&legacy_migrations()));
    assert_eq!(
        digest(&legacy_migrations()),
        LEGACY_MIGRATION_DIGEST,
        "the legacy list changed. It is frozen, so a new migration is always linted."
    );
}

#[test]
fn a_new_backdated_migration_is_in_scope() {
    let at = |tree: &'static str, name: &str| OnDisk {
        tree,
        name: name.to_string(),
        sql: String::new(),
        run_in_transaction: true,
    };
    // A version at or before the cutoff does not exempt a new migration.
    assert!(at("autumn-harvest/migrations", "20260914010101_new_change").in_scope());
    let legacy = load_migrations()
        .into_iter()
        .find(|m| m.version() <= LOCK_SAFETY_CUTOFF)
        .expect("a legacy migration is on disk");
    assert!(!legacy.in_scope(), "{}", legacy.name);
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
    let all = lint_all(&migrations);
    for (name, rule, _) in GRANDFATHERED {
        let at = migrations
            .iter()
            .position(|m| m.name == *name)
            .unwrap_or_else(|| panic!("{name} is not on disk"));
        assert!(
            rules(&all[at]).contains(rule),
            "{name} no longer breaks {}. Remove its GRANDFATHERED entry.",
            rule.id()
        );
    }
}

// ── CI wiring and docs ───────────────────────────────────────────────────────

/// The lint must run in an ungated step of the `lint` job.
///
/// The step must be one plain `cargo test` of the whole module. A gated,
/// soft-failing, chained or partial run does not count.
#[test]
fn the_lint_runs_in_the_ci_lint_job() {
    let doc = parse_workflow(".github/workflows/ci.yml");
    let steps = doc
        .get("jobs")
        .and_then(|jobs| jobs.get("lint"))
        .and_then(|lint| lint.get("steps"))
        .and_then(serde_yaml::Value::as_sequence)
        .expect("ci.yml has a lint job with steps");
    let runs = steps
        .iter()
        .filter(|step| ungated(step))
        .filter_map(|step| step.get("run").and_then(serde_yaml::Value::as_str))
        .any(|run| {
            let words: Vec<&str> = run.split_whitespace().collect();
            run.trim_start().starts_with("cargo test ")
                && words
                    .windows(3)
                    .any(|w| w == ["--test", "integration", "migration_lock_safety::"])
                && !SHELL_OPERATORS.iter().any(|op| run.trim().contains(op))
                && !words
                    .iter()
                    .any(|word| NO_FULL_RUN_FLAGS.iter().any(|flag| word.starts_with(flag)))
        });
    assert!(
        runs,
        "an ungated step of the lint job must run \
         `cargo test ... --test integration migration_lock_safety::`"
    );
}

/// The author guide, with line endings normalised for a Windows checkout.
fn read_guide() -> String {
    let path = workspace_root().join("docs/upgrading/online-migrations.md");
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
        .replace("\r\n", "\n")
}

/// The author guide names every hot table and every rule the lint enforces.
#[test]
fn the_author_guide_matches_the_lint() {
    let guide = read_guide();
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
    let guide = read_guide();
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
