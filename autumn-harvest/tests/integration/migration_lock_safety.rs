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
    _sql: &str,
    _run_in_transaction: bool,
    _index_tables: &BTreeMap<String, String>,
) -> Vec<Finding> {
    Vec::new()
}

/// Map each index that a migration creates to its table.
fn index_tables<'a>(_sqls: impl IntoIterator<Item = &'a str>) -> BTreeMap<String, String> {
    BTreeMap::new()
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
    let sql =
        std::fs::read_to_string(&up).unwrap_or_else(|e| panic!("read {}: {e}", up.display()));
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
        assert_eq!(rules(&findings), [Rule::BlockingIndex], "{sql}: {findings:?}");
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
    let history = ["CREATE INDEX idx_hot ON harvest_workflow_executions (id);\n\
                    CREATE INDEX idx_cold ON harvest_schedules (id);"];

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
    assert!(!run_in_transaction("# no transaction\nrun_in_transaction = false\n"));
    assert!(!run_in_transaction("run_in_transaction=false # CONCURRENTLY\n"));
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
    assert!(rules(&findings).contains(&Rule::LockTimeout), "{findings:?}");

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
        assert!(rule.allowable(), "{name}: {} cannot be grandfathered", rule.id());
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
    let step_start = block[..at].rfind("\n      - ").expect("the run line is in a step");
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
    let guide = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    for table in HOT_TABLES {
        assert!(guide.contains(&format!("`{table}`")), "the guide must list {table}");
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
