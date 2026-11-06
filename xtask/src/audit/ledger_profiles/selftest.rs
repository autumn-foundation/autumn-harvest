//! The fixtures of `cargo xtask audit ledger-profiles --self-test`.
//!
//! They cover the forms that the scan must get right: comments, strings, raw
//! strings, lifetimes, atomics, `impl` blocks, macros and test modules. The
//! unit tests run the same fixtures, so `cargo test -p xtask` covers them.

use std::collections::{BTreeMap, BTreeSet};

use smop::prelude::*;

use super::scan::{Scanner, lex};
use super::{DRIVERS, findings, pyrepr, scaffold};

/// A Postgres source file with each tricky form.
pub const FIXTURE: &str = r##"
use diesel_async::RunQueryDsl;

const NOTE: &str = "a string that says .execute(conn) is not a call";

pub async fn claim(conn: &mut AsyncPgConnection) -> QueryResult<usize> {
    // conn.batch_execute("DROP TABLE harvest_jobs") in a comment is not a call.
    let n = counter.load(Ordering::SeqCst);
    sql_query("UPDATE harvest_jobs SET state = 'RUNNING' WHERE id = $1")
        .execute(conn)
        .await
}

impl<'a> Reader for Store<'a> {
    async fn depth(&self, conn: &mut AsyncPgConnection) -> i64 {
        let label = 'x';
        harvest_jobs::table.count().get_result(conn).await.unwrap_or(0)
    }
}

macro_rules! count_rows {
    ($conn:expr) => {
        sql_query(r#"SELECT count(*) FROM harvest_jobs"#).load($conn)
    };
}

fn helper(items: &[u8]) -> Option<&u8> {
    items.first()
}

const PURGE_SQL: &str = "DELETE FROM harvest_archive WHERE id = $1";

fn archive_query() -> String {
    format!("SELECT id FROM harvest_archive LIMIT {}", 10)
}

pub async fn newest(conn: &mut AsyncPgConnection) -> QueryResult<(i64, String)> {
    harvest_jobs::table.select((id, state)).first::<(i64, String)>(conn).await
}

macro_rules! park_sql {
    () => {
        concat!("UPDATE harvest_jobs j \
         ", "SET state = 'PARKED'")
    };
}

pub async fn park(conn: &mut AsyncPgConnection) -> QueryResult<usize> {
    sql_query(park_sql!()).execute(conn).await
}

pub async fn purge(conn: &mut AsyncPgConnection) -> QueryResult<()> {
    let rows: Vec<IdRow> = sql_query(archive_query()).load(conn).await?;
    cache.load(&slot, rows.len());
    sql_query(PURGE_SQL).execute(conn).await.map(drop)
}

#[cfg(test)]
mod tests {
    #[test]
    fn seeds() {
        sql_query("INSERT INTO harvest_jobs DEFAULT VALUES").execute(conn);
    }
}
"##;

/// A `rusqlite` source file.
pub const SQLITE_FIXTURE: &str = r#"
use rusqlite::Connection;

pub fn open(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch("CREATE TABLE IF NOT EXISTS harvest_jobs (id INTEGER)")?;
    conn.execute("DELETE FROM harvest_jobs", [])?;
    Ok(())
}
"#;

/// A migration that creates both fixture tables.
pub const MIGRATION: &str = "CREATE TABLE harvest_jobs (id BIGINT PRIMARY KEY);
CREATE UNLOGGED TABLE IF NOT EXISTS harvest_archive (id BIGINT);";

fn set(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(ToString::to_string).collect()
}

fn profile(
    driver: &str,
    kind: &[&str],
    tables: &[&str],
    measured: &str,
    plan: Option<&str>,
) -> toml::Value {
    let list = |items: &[&str]| {
        toml::Value::Array(
            items
                .iter()
                .map(|s| toml::Value::String((*s).to_string()))
                .collect(),
        )
    };
    let mut table = toml::Table::new();
    table.insert("driver".into(), toml::Value::String(driver.into()));
    table.insert("backend".into(), list(&["postgres"]));
    table.insert("kind".into(), list(kind));
    table.insert("tables".into(), list(tables));
    table.insert("measured".into(), toml::Value::String(measured.into()));
    if let Some(plan) = plan {
        table.insert("plan".into(), toml::Value::String(plan.into()));
    }
    toml::Value::Table(table)
}

fn good_catalog() -> toml::Table {
    let mut good = toml::Table::new();
    good.insert(
        "claim".into(),
        profile(
            "per_task",
            &["write"],
            &["harvest_jobs"],
            "docs/performance-claim.md#x",
            Some("docs/ledger/plans/c/queue.md"),
        ),
    );
    good.insert(
        "Store::depth".into(),
        profile("per_tick", &["read"], &["harvest_jobs"], "unmeasured", None),
    );
    good.insert(
        "count_rows!".into(),
        profile(
            "per_request",
            &["read"],
            &["harvest_jobs"],
            "unmeasured",
            None,
        ),
    );
    good.insert(
        "newest".into(),
        profile(
            "per_request",
            &["read"],
            &["harvest_jobs"],
            "unmeasured",
            None,
        ),
    );
    good.insert(
        "park".into(),
        profile(
            "per_task",
            &["write"],
            &["harvest_jobs"],
            "unmeasured",
            None,
        ),
    );
    good.insert(
        "purge".into(),
        profile(
            "setup",
            &["read", "write"],
            &["harvest_archive"],
            "unmeasured",
            None,
        ),
    );
    good
}

fn with_fields(value: &toml::Value, fields: &[(&str, toml::Value)]) -> toml::Value {
    let mut table = value.as_table().cloned().unwrap_or_default();
    for (name, field) in fields {
        table.insert((*name).to_string(), field.clone());
    }
    toml::Value::Table(table)
}

fn get<'a>(table: &'a toml::Table, key: &str) -> Result<&'a toml::Value> {
    table.get(key).with_context(|| format!("fixture key {key}"))
}

fn expect_findings(
    scanner: &Scanner,
    sources: &BTreeMap<String, String>,
    catalogs: &BTreeMap<String, toml::Table>,
    expected: &[String],
) -> Result<()> {
    let pages: BTreeMap<&str, &str> = BTreeMap::from([
        ("docs/performance-claim.md", "Ledger measured `claim` here."),
        (
            "docs/ledger/plans/c/queue.md",
            "## `claim`\n\n| Statement |",
        ),
    ]);
    let read_text =
        |rel: &str| -> Result<Option<String>> { Ok(pages.get(rel).map(|s| (*s).to_string())) };
    let found = findings(scanner, sources, catalogs, &read_text)?;
    ensure!(found == expected, "findings {found:#?} != {expected:#?}");
    Ok(())
}

/// Runs every fixture, and returns the first assertion that fails.
pub fn self_test(scanner: &Scanner) -> Result<()> {
    let tables = scanner.known_tables([("m/migrations/1/up.sql", MIGRATION)]);
    ensure!(
        tables == set(&["harvest_jobs", "harvest_archive"]),
        "{tables:?}"
    );

    let (code, full) = lex(FIXTURE);
    ensure!(code.len() == FIXTURE.len() && full.len() == FIXTURE.len());

    let sites = scanner.scan_source(FIXTURE, &tables);
    let keys: BTreeSet<String> = sites.keys().cloned().collect();
    let want = set(&[
        "claim",
        "Store::depth",
        "count_rows!",
        "newest",
        "park",
        "purge",
    ]);
    ensure!(keys == want, "{sites:?}");
    let site = |key: &str| sites.get(key).with_context(|| format!("site {key}"));
    ensure!(
        site("newest")?.kind == set(&["read"]),
        "{:?}",
        site("newest")?
    );
    ensure!(site("park")?.kind == set(&["write"]), "{:?}", site("park")?);
    ensure!(
        site("park")?.tables == set(&["harvest_jobs"]),
        "{:?}",
        site("park")?
    );
    ensure!(
        site("purge")?.kind == set(&["read", "write"]),
        "{:?}",
        site("purge")?
    );
    ensure!(
        site("purge")?.tables == set(&["harvest_archive"]),
        "{:?}",
        site("purge")?
    );
    ensure!(
        site("claim")?.backend == set(&["postgres"]),
        "{:?}",
        site("claim")?
    );
    ensure!(
        site("claim")?.kind == set(&["write"]),
        "{:?}",
        site("claim")?
    );
    ensure!(
        site("claim")?.tables == set(&["harvest_jobs"]),
        "{:?}",
        site("claim")?
    );
    ensure!(
        site("Store::depth")?.kind == set(&["read"]),
        "{:?}",
        site("Store::depth")?
    );
    ensure!(
        site("count_rows!")?.kind == set(&["read"]),
        "{:?}",
        site("count_rows!")?
    );

    let sites = scanner.scan_source(SQLITE_FIXTURE, &tables);
    let keys: BTreeSet<String> = sites.keys().cloned().collect();
    ensure!(keys == set(&["open"]), "{sites:?}");
    let open = sites.get("open").context("site open")?;
    ensure!(open.backend == set(&["sqlite"]), "{open:?}");
    ensure!(open.kind == set(&["ddl", "write"]), "{open:?}");

    check_findings(scanner)
}

/// Runs the gate fixtures: a good catalog and each kind of defect.
fn check_findings(scanner: &Scanner) -> Result<()> {
    let sources: BTreeMap<String, String> = [
        ("c/src/queue.rs", FIXTURE),
        ("c/src/lib.rs", "#[cfg(test)]\nmod fixtures;\n"),
        ("c/src/fixtures.rs", SQLITE_FIXTURE),
        ("c/migrations/1/up.sql", MIGRATION),
    ]
    .into_iter()
    .map(|(p, s)| (p.to_string(), s.to_string()))
    .collect();
    let catalog = "docs/ledger/c/queue.toml";
    let good = good_catalog();
    expect_findings(
        scanner,
        &sources,
        &BTreeMap::from([(catalog.to_string(), good.clone())]),
        &[],
    )?;

    let mut missing = good.clone();
    missing.remove("claim");
    expect_findings(
        scanner,
        &sources,
        &BTreeMap::from([(catalog.to_string(), missing)]),
        &["docs/ledger/c/queue.toml: no profile for `claim` (c/src/queue.rs:9)".to_string()],
    )?;

    let mut stale = good.clone();
    stale.insert("old_name".into(), get(&good, "claim")?.clone());
    expect_findings(
        scanner,
        &sources,
        &BTreeMap::from([
            (catalog.to_string(), stale),
            ("docs/ledger/c/gone.toml".to_string(), toml::Table::new()),
        ]),
        &[
            "docs/ledger/c/queue.toml: profile [\"old_name\"] matches no query site in c/src/queue.rs"
                .to_string(),
            "docs/ledger/c/gone.toml: no source file has query sites for this catalog".to_string(),
        ],
    )?;

    check_bad_profiles(scanner, &sources, catalog, &good)
}

/// Runs the fixtures of a wrong field, a bad page and the scaffold.
fn check_bad_profiles(
    scanner: &Scanner,
    sources: &BTreeMap<String, String>,
    catalog: &str,
    good: &toml::Table,
) -> Result<()> {
    let mut wrong = good.clone();
    wrong.insert(
        "claim".into(),
        with_fields(
            get(good, "claim")?,
            &[
                ("driver", toml::Value::String("often".into())),
                (
                    "kind",
                    toml::Value::Array(vec![toml::Value::String("read".into())]),
                ),
                ("tables", toml::Value::Array(Vec::new())),
                (
                    "measured",
                    toml::Value::String("docs/performance-other.md".into()),
                ),
                ("extra", toml::Value::Integer(1)),
            ],
        ),
    );
    wrong.insert(
        "Store::depth".into(),
        with_fields(
            get(good, "Store::depth")?,
            &[(
                "measured",
                toml::Value::String("docs/performance-claim.md".into()),
            )],
        ),
    );
    let drivers: Vec<&str> = DRIVERS.iter().map(|(name, _)| *name).collect();
    expect_findings(
        scanner,
        sources,
        &BTreeMap::from([(catalog.to_string(), wrong)]),
        &[
            "docs/ledger/c/queue.toml: [\"Store::depth\"] measured page docs/performance-claim.md \
             does not name `depth`"
                .to_string(),
            "docs/ledger/c/queue.toml: [\"claim\"] has unknown field(s) ['extra']".to_string(),
            format!(
                "docs/ledger/c/queue.toml: [\"claim\"] driver 'often' is not one of {}",
                pyrepr::list_repr(&drivers)
            ),
            "docs/ledger/c/queue.toml: [\"claim\"] kind omits detected ['write']".to_string(),
            "docs/ledger/c/queue.toml: [\"claim\"] tables omits detected ['harvest_jobs']"
                .to_string(),
            "docs/ledger/c/queue.toml: [\"claim\"] measured page docs/performance-other.md does \
             not exist"
                .to_string(),
        ],
    )?;

    let mut planless = good.clone();
    planless.insert(
        "count_rows!".into(),
        with_fields(
            get(good, "count_rows!")?,
            &[(
                "plan",
                toml::Value::String("docs/ledger/plans/c/queue.md".into()),
            )],
        ),
    );
    planless.insert(
        "purge".into(),
        with_fields(
            get(good, "purge")?,
            &[(
                "plan",
                toml::Value::String("docs/ledger/plans/c/gone.md".into()),
            )],
        ),
    );
    expect_findings(
        scanner,
        sources,
        &BTreeMap::from([(catalog.to_string(), planless)]),
        &[
            "docs/ledger/c/queue.toml: [\"count_rows!\"] plan page docs/ledger/plans/c/queue.md \
             has no section for `count_rows!`"
                .to_string(),
            "docs/ledger/c/queue.toml: [\"purge\"] plan page docs/ledger/plans/c/gone.md does not \
             exist"
                .to_string(),
        ],
    )?;

    let only_queue = BTreeMap::from([("c/src/queue.rs".to_string(), FIXTURE.to_string())]);
    let stub = scaffold(scanner, &only_queue, &BTreeMap::new());
    ensure!(
        stub.contains("[\"claim\"]\ndriver = \"CHOOSE\"\nbackend = [\"postgres\"]"),
        "{stub}"
    );
    Ok(())
}
