//! The fixtures of `cargo xtask audit ledger-census --self-test`.
//!
//! CI runs them in the `lint` job. The unit tests run them too, so
//! `cargo test -p xtask` covers them. They need no database: the SQLite
//! fixtures use an in-memory database.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;

use rusqlite::Connection;
use serde_json::json;
use smop::prelude::*;

use super::attribute::{
    Entry, Literals, Patterns, Resolver, Statements, attribute, line_starts, merge, shape,
    split_top, tagged,
};
use super::plan::{Row, Section, access_paths, render_page, set_plan_field, source_of, unindexed};
use super::sqlite::{Params, SQLITE_DDL, SqlitePatterns, schema};
use super::{FULL_INDEX, NO_INDEX, PARTIAL_INDEX, SQLITE_SECTION, plannable};
use crate::audit::ledger_profiles::scan::{Scanner, lex};

const PLAN_JSON: &str = r#"[{"Plan": {"Node Type": "Limit", "Plans": [
  {"Node Type": "Index Scan", "Relation Name": "harvest_task_queue", "Index Name": "idx_tq_poll",
   "Index Cond": "(id = $1)"},
  {"Node Type": "Seq Scan", "Relation Name": "harvest_workers"},
  {"Node Type": "Index Only Scan", "Relation Name": "harvest_timers", "Index Name": "harvest_timers_pkey"},
  {"Node Type": "Bitmap Heap Scan", "Relation Name": "harvest_events",
   "Plans": [{"Node Type": "Bitmap Index Scan", "Index Name": "idx_ev"}]}]}}]"#;

fn pair(table: &str, access: &str) -> (String, String) {
    (table.to_string(), access.to_string())
}

fn lits(items: &[(&str, &str)]) -> Literals {
    items
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

fn entry(types: &[(&str, &str)], literals: Vec<Literals>) -> Entry {
    Entry {
        types: lits(types),
        literals,
        extra: BTreeMap::new(),
    }
}

fn statements(items: &[(&str, &str, &str, Entry)]) -> Statements {
    let mut out = Statements::new();
    for (catalog, key, sql, value) in items {
        out.entry((*catalog).to_string())
            .or_default()
            .entry((*key).to_string())
            .or_default()
            .insert((*sql).to_string(), value.clone());
    }
    out
}

const fn no_set() -> BTreeSet<String> {
    BTreeSet::new()
}

fn check_plans() -> Result<()> {
    let none = no_set();
    let found = access_paths(PLAN_JSON, &none)?;
    ensure!(
        found
            == vec![
                pair("harvest_events", "Bitmap Heap Scan on `idx_ev`"),
                pair("harvest_task_queue", "Index Scan on `idx_tq_poll`"),
                pair(
                    "harvest_timers",
                    &format!("{FULL_INDEX} `harvest_timers_pkey`")
                ),
                pair("harvest_workers", NO_INDEX),
            ],
        "{found:?}"
    );
    let nested = json!([{"Plan": {"Node Type": "Bitmap Heap Scan", "Relation Name": "q", "Plans": [
        {"Node Type": "Index Scan", "Relation Name": "w", "Index Name": "w_pkey", "Index Cond": "(id = $1)"},
        {"Node Type": "Bitmap Index Scan", "Index Name": "q_idx"}]}}])
    .to_string();
    let found = access_paths(&nested, &none)?;
    ensure!(
        found.contains(&pair("q", "Bitmap Heap Scan on `q_idx`")),
        "{found:?}"
    );
    let full = json!([{"Plan": {"Node Type": "Index Scan", "Relation Name": "q", "Index Name": "q_running"}}])
        .to_string();
    let partial = BTreeSet::from(["q_running".to_string()]);
    let found = access_paths(&full, &partial)?;
    ensure!(
        found == vec![pair("q", &format!("{PARTIAL_INDEX} `q_running`"))],
        "{found:?}"
    );
    ensure!(!unindexed(&format!("{PARTIAL_INDEX} `q_running`")));
    ensure!(
        unindexed(NO_INDEX)
            && !unindexed(&format!("{FULL_INDEX} `x`"))
            && !unindexed("Index Scan on `x`")
    );

    let plannable = plannable()?;
    ensure!(plannable.is_match("  WITH c AS (SELECT 1) SELECT * FROM c"));
    ensure!(plannable.is_match("(SELECT 1) UNION (SELECT 2)"));
    ensure!(!plannable.is_match("SET LOCAL lock_timeout = '5s'"));
    ensure!(!plannable.is_match("BEGIN"));
    ensure!(
        plannable.is_match("/* claim: priority ageing */ WITH c AS (SELECT 1) SELECT * FROM c")
    );
    ensure!(plannable.is_match("-- note\n  SELECT 1"));
    ensure!(!plannable.is_match("/* x */ SET LOCAL a = 1"));
    Ok(())
}

fn check_catalog_edits(patterns: &Patterns) -> Result<()> {
    let normal = patterns.normalize("SELECT a -- why\n      FROM t\n\n  WHERE  b");
    ensure!(normal == "SELECT a -- why\nFROM t\nWHERE b", "{normal:?}");

    let text = "[\"a\"]\ndriver = \"per_task\"\nmeasured = \"unmeasured\"\n\n[\"b\"]\nmeasured = \"unmeasured\"\n";
    let once = set_plan_field(text, "a", "docs/ledger/plans/x.md")?;
    let want = "[\"a\"]\ndriver = \"per_task\"\nmeasured = \"unmeasured\"\nplan = \"docs/ledger/plans/x.md\"\n\
                \n[\"b\"]\nmeasured = \"unmeasured\"\n";
    ensure!(once == want, "{once:?}");
    ensure!(
        set_plan_field(&once, "a", "docs/ledger/plans/y.md")?
            .matches("plan = ")
            .count()
            == 1
    );
    ensure!(
        set_plan_field(text, "b", "p.md")?
            .ends_with("measured = \"unmeasured\"\nplan = \"p.md\"\n")
    );
    ensure!(
        source_of("docs/ledger/autumn-harvest/dst/mod.toml")? == "autumn-harvest/src/dst/mod.rs"
    );
    Ok(())
}

fn check_attribute(scanner: &Scanner, patterns: &Patterns) -> Result<()> {
    let source = "pub async fn claim(conn: &mut C) {\n    let x = 1;\n    helper(conn).await;\n}\n\nfn helper(c: &mut C) {\n    c.go();\n}\n";
    let mut claim = toml::Table::new();
    claim.insert("claim".into(), toml::Value::Table(toml::Table::new()));
    let catalogs = BTreeMap::from([("docs/ledger/c/queue.toml".to_string(), claim)]);
    let mut resolver = Resolver::new(Path::new("/fixture"), &catalogs, scanner);
    resolver.cache.insert(
        "c/src/queue.rs".into(),
        Some((line_starts(source), scanner.frames_of(&lex(source).0))),
    );
    let hit = resolver.key("c/src/queue.rs:2")?;
    ensure!(
        hit == Some(("docs/ledger/c/queue.toml".into(), "claim".into())),
        "{hit:?}"
    );
    ensure!(resolver.key("c/src/queue.rs:7")?.is_none());
    let line = json!({"sql": "SELECT\n    1", "frames": ["c/src/queue.rs:7", "c/src/queue.rs:3"]})
        .to_string();
    let found = attribute([line.as_str()], &mut resolver, patterns, None)?;
    let want = statements(&[(
        "docs/ledger/c/queue.toml",
        "claim",
        "SELECT\n1",
        entry(&[], vec![Literals::new()]),
    )]);
    ensure!(found == want, "{found:?}");
    let mut torn = Vec::new();
    let with_torn = attribute(
        [line.as_str(), r#"{"sql": "SEL{"sql""#],
        &mut resolver,
        patterns,
        Some(&mut torn),
    )?;
    ensure!(with_torn == want, "{with_torn:?}");
    ensure!(torn.len() == 1, "{torn:?}");
    Ok(())
}

fn check_merge_and_literals(patterns: &Patterns) -> Result<()> {
    let e = entry(&[], vec![Literals::new()]);
    let t = entry(&[("2", "uuid")], vec![lits(&[("1", "true")])]);
    let merged = merge(
        &statements(&[("c", "k", "a", e.clone())]),
        &statements(&[
            ("c", "k", "b", e.clone()),
            ("c", "k", "a", t),
            ("c", "j", "c", e.clone()),
        ]),
    );
    let want = statements(&[
        ("c", "j", "c", e.clone()),
        (
            "c",
            "k",
            "a",
            entry(
                &[("2", "uuid")],
                vec![lits(&[("1", "true")]), Literals::new()],
            ),
        ),
        ("c", "k", "b", e),
    ]);
    ensure!(merged == want, "{merged:?}");
    let shaped = shape(&[
        lits(&[("::2", "uuid"), ("1", "true")]),
        lits(&[("::2", "uuid")]),
    ]);
    ensure!(
        shaped
            == entry(
                &[("2", "uuid")],
                vec![lits(&[("1", "true")]), Literals::new()]
            ),
        "{shaped:?}"
    );

    let binds = format!(
        r#"[0b1c-uuid, false, "RUNNING", Some(true), ["A", "B"], Object {{"a": String("x, y")}}, "it's", "test-{}"]"#,
        "0a".repeat(16)
    );
    let found: Literals = patterns
        .literals(&binds)
        .into_iter()
        .filter(|(k, _)| !k.starts_with("::"))
        .collect();
    let want = lits(&[
        ("2", "false"),
        ("3", "'RUNNING'"),
        ("4", "true"),
        ("5", "ARRAY['A', 'B']"),
    ]);
    ensure!(found == want, "{found:?}");
    let typed = patterns.literals(
        "[1f0c3a2e-0000-4000-8000-000000000001, 7, [2026-10-28T17:35:07.25Z], None, Object {}]",
    );
    let want = lits(&[
        ("::1", "uuid"),
        ("::2", "bigint"),
        ("::3", "timestamptz[]"),
        ("::5", "jsonb"),
    ]);
    ensure!(typed == want, "{typed:?}");
    let cast: HashMap<String, String> = [("1", "true"), ("::2", "timestamptz[]")]
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    let swapped = patterns.substitute("SELECT ($2)[1] WHERE a = $1", &cast, true);
    ensure!(
        swapped == "SELECT ($1::timestamptz[])[1] WHERE a = true",
        "{swapped}"
    );
    ensure!(patterns.literals("[]").is_empty() && patterns.literals("").is_empty());
    let long = format!(r#"["{}"]"#, "x".repeat(80));
    ensure!(patterns.literals(&long) == lits(&[("::1", "text")]));
    ensure!(split_top(r#"a, [b, c], "d, e""#) == vec!["a", "[b, c]", r#""d, e""#]);
    let two: HashMap<String, String> = HashMap::from([("2".to_string(), "false".to_string())]);
    let renumbered = patterns.substitute(
        "SELECT 1 WHERE a = $1 AND b = $2 AND c = $3 AND d = $10",
        &two,
        false,
    );
    ensure!(
        renumbered == "SELECT 1 WHERE a = $1 AND b = false AND c = $2 AND d = $3",
        "{renumbered}"
    );
    Ok(())
}

fn check_sqlite(sq: &SqlitePatterns) -> Result<()> {
    let tables = BTreeSet::from([
        "harvest_tasks".to_string(),
        "harvest_executions".to_string(),
    ]);
    let none = no_set();
    let no_alias = BTreeMap::new();
    let access = |detail: &str, partial: &BTreeSet<String>, aliases: &BTreeMap<String, String>| {
        sq.access(detail, &tables, partial, aliases)
    };
    ensure!(
        access("SCAN harvest_tasks", &none, &no_alias) == Some(pair("harvest_tasks", NO_INDEX))
    );
    ensure!(access("SCAN t", &none, &no_alias).is_none());
    ensure!(
        access(
            "SCAN harvest_tasks AS t USING COVERING INDEX idx_seq",
            &none,
            &no_alias
        ) == Some(pair("harvest_tasks", &format!("{FULL_INDEX} `idx_seq`")))
    );
    let live = BTreeSet::from(["idx_live".to_string()]);
    ensure!(
        access("SCAN harvest_tasks USING INDEX idx_live", &live, &no_alias)
            == Some(pair(
                "harvest_tasks",
                &format!("{PARTIAL_INDEX} `idx_live`")
            ))
    );
    ensure!(
        access(
            "SEARCH harvest_executions USING INDEX idx_key (exec_id=?)",
            &none,
            &no_alias
        ) == Some(pair("harvest_executions", "Search on `idx_key`"))
    );
    ensure!(
        access(
            "SEARCH harvest_tasks USING INTEGER PRIMARY KEY (rowid=?)",
            &none,
            &no_alias
        ) == Some(pair("harvest_tasks", "Search on the primary key"))
    );
    ensure!(access("USE TEMP B-TREE FOR ORDER BY", &none, &no_alias).is_none());
    ensure!(
        access("SEARCH harvest_tasks", &none, &no_alias) == Some(pair("harvest_tasks", NO_INDEX))
    );
    let aliases = sq.aliases(
        "SELECT 1 FROM harvest_tasks t JOIN harvest_executions AS e ON 1 WHERE x",
        &tables,
    );
    let want: BTreeMap<String, String> = [("t", "harvest_tasks"), ("e", "harvest_executions")]
        .iter()
        .map(|(a, t)| ((*a).to_string(), (*t).to_string()))
        .collect();
    ensure!(aliases == want, "{aliases:?}");
    ensure!(
        sq.aliases("SELECT 1 FROM harvest_tasks WHERE x", &tables)
            .is_empty()
    );
    let t_alias = BTreeMap::from([("t".to_string(), "harvest_tasks".to_string())]);
    ensure!(access("SCAN t", &none, &t_alias) == Some(pair("harvest_tasks", NO_INDEX)));
    let e_alias = BTreeMap::from([("e".to_string(), "harvest_executions".to_string())]);
    ensure!(
        access("SEARCH e USING INDEX k (id=?)", &none, &e_alias)
            == Some(pair("harvest_executions", "Search on `k`"))
    );

    Ok(())
}

fn check_sqlite_plans(sq: &SqlitePatterns) -> Result<()> {
    let none = no_set();
    let db_max = Connection::open_in_memory().context("opening an in-memory SQLite database")?;
    ensure!(sq.explain(&db_max, "SELECT 1", &none, &none).1.is_none());
    db_max
        .execute_batch("CREATE TABLE m (a INTEGER, b INTEGER); CREATE INDEX m_b ON m (b);")
        .context("building the MAX fixture")?;
    let m = BTreeSet::from(["m".to_string()]);
    let got = sq.explain(&db_max, "SELECT MAX(a) FROM m", &m, &none);
    ensure!(got == (Some(vec![pair("m", NO_INDEX)]), None), "{got:?}");
    let got = sq.explain(&db_max, "SELECT MAX(b) FROM m", &m, &none);
    ensure!(
        got == (Some(vec![pair("m", "Search on `m_b`")]), None),
        "{got:?}"
    );

    let create = sq.ddl_kind("-- why\nCREATE TABLE IF NOT EXISTS t (a)");
    let index = sq.ddl_kind("CREATE INDEX IF NOT EXISTS i ON t (a)");
    ensure!(
        create.is_some() && index.is_some() && create < index,
        "{create:?} {index:?}"
    );
    ensure!(
        sq.ddl_kind("ALTER TABLE t ADD COLUMN b")
            == SQLITE_DDL.iter().position(|k| *k == "ALTER TABLE")
    );
    ensure!(sq.ddl_kind("SELECT 1").is_none());
    ensure!(sq.params("SELECT 1 WHERE a = ? AND b = ?") == Params::Positional(2));
    ensure!(sq.params("SELECT 1 WHERE a = ?3") == Params::Positional(3));
    ensure!(sq.params("SELECT 1 WHERE a = ?1 AND b = ?2") == Params::Positional(2));
    ensure!(
        sq.params("SELECT 1 WHERE a = :id") == Params::Named(BTreeSet::from(["id".to_string()]))
    );

    Ok(())
}

fn check_sqlite_schema(sq: &SqlitePatterns) -> Result<()> {
    let none = no_set();
    let mut sqlite_entry = entry(&[], vec![Literals::new()]);
    sqlite_entry.extra.insert("backend".into(), json!("sqlite"));
    let fixture = statements(&[
        (
            "c",
            "k",
            "CREATE TABLE IF NOT EXISTS q (id INTEGER PRIMARY KEY, s TEXT)",
            sqlite_entry.clone(),
        ),
        (
            "c",
            "k",
            "CREATE INDEX IF NOT EXISTS q_s ON q (s) WHERE s IS NOT NULL",
            sqlite_entry.clone(),
        ),
        (
            "c",
            "k",
            "CREATE TABLE pg_only (a int)",
            entry(&[], vec![Literals::new()]),
        ),
    ]);
    let (db, found_tables, found_partial) = schema(&fixture, sq)?;
    ensure!(
        found_tables == BTreeSet::from(["q".to_string()])
            && found_partial == BTreeSet::from(["q_s".to_string()]),
        "{found_tables:?} {found_partial:?}"
    );
    let got = sq.explain(
        &db,
        "SELECT id FROM q WHERE s = ?",
        &found_tables,
        &found_partial,
    );
    ensure!(
        got == (Some(vec![pair("q", "Search on `q_s`")]), None),
        "{got:?}"
    );
    let got = sq.explain(&db, "SELECT id FROM q WHERE s LIKE ?", &found_tables, &none);
    ensure!(got == (Some(vec![pair("q", NO_INDEX)]), None), "{got:?}");
    let got = sq.explain(
        &db,
        "SELECT id FROM q WHERE s = ? AND s <> '?'",
        &found_tables,
        &found_partial,
    );
    ensure!(got.1.is_none(), "{got:?}");
    let got = sq.explain(&db, "SELECT nope FROM q", &found_tables, &none);
    ensure!(
        got.1
            .as_deref()
            .is_some_and(|e| e.starts_with("no such column")),
        "{got:?}"
    );

    let e = entry(&[], vec![Literals::new()]);
    ensure!(tagged(e.clone(), true) == sqlite_entry && tagged(e.clone(), false) == e);
    let merged = merge(
        &statements(&[("c", "k", "a", sqlite_entry.clone())]),
        &statements(&[("c", "k", "a", e)]),
    );
    ensure!(
        merged == statements(&[("c", "k", "a", sqlite_entry)]),
        "{merged:?}"
    );
    let row = |sql: &str, paths: Vec<(String, String)>, sqlite: bool| Row {
        sql: sql.to_string(),
        paths: Some(paths),
        error: None,
        sqlite,
    };
    let page = render_page(
        "x/src/y.rs",
        &BTreeMap::from([(
            "k".to_string(),
            Section::Rows(vec![row(
                "SELECT 1 FROM q",
                vec![pair("q", NO_INDEX)],
                true,
            )]),
        )]),
    )?;
    ensure!(
        page.contains(SQLITE_SECTION)
            && page.contains("EXPLAIN QUERY PLAN")
            && !page.contains("GENERIC_PLAN")
    );
    let page = render_page(
        "x/src/y.rs",
        &BTreeMap::from([(
            "k".to_string(),
            Section::Rows(vec![row("SELECT 1", Vec::new(), false)]),
        )]),
    )?;
    ensure!(!page.contains("Each Postgres"));
    Ok(())
}

/// Runs every fixture, and returns the first assertion that fails.
pub fn self_test() -> Result<()> {
    let scanner = Scanner::new()?;
    let patterns = Patterns::new()?;
    check_plans()?;
    check_catalog_edits(&patterns)?;
    check_attribute(&scanner, &patterns)?;
    check_merge_and_literals(&patterns)?;
    let sq = SqlitePatterns::new()?;
    check_sqlite(&sq)?;
    check_sqlite_plans(&sq)?;
    check_sqlite_schema(&sq)?;
    Ok(())
}
