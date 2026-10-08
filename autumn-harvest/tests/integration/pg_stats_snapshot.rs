//! Snapshot `pg_stat_statements` and `pg_stat_user_tables` (issue #1956).
//!
//! `pg_stat_user_tables` lives in the database it describes. A harness that
//! drops its database at exit loses those counters. So a harness calls
//! [`capture`] first and drops the database after it.
//!
//! `pg_stat_statements` is cluster-wide. Every read here filters on the dbid
//! of the current database. [`reset_statements`] clears only that dbid. A new
//! database can reuse the OID of a dropped one, and the old rows stay in the
//! view until a reset.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Double, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};

/// How long [`capture`] waits for other sessions on the database to end.
///
/// A session flushes its pending table counters when it ends. A snapshot taken
/// while a pool still closes can miss the last few statements.
const QUIESCE_BOUND: Duration = Duration::from_secs(10);

/// One `pg_stat_statements` row, scoped to one database.
#[derive(Debug, Clone, PartialEq, QueryableByName)]
pub struct StatementStats {
    #[diesel(sql_type = Text)]
    pub query: String,
    #[diesel(sql_type = BigInt)]
    pub calls: i64,
    #[diesel(sql_type = BigInt)]
    pub rows: i64,
    #[diesel(sql_type = Double)]
    pub total_exec_ms: f64,
    #[diesel(sql_type = BigInt)]
    pub shared_blks_hit: i64,
    #[diesel(sql_type = BigInt)]
    pub shared_blks_read: i64,
    #[diesel(sql_type = BigInt)]
    pub shared_blks_dirtied: i64,
    #[diesel(sql_type = BigInt)]
    pub shared_blks_written: i64,
    #[diesel(sql_type = BigInt)]
    pub temp_blks_written: i64,
}

impl StatementStats {
    /// Shared buffers hit plus read. The issue #1956 profile ranks by this.
    #[must_use]
    pub const fn total_buffers(&self) -> i64 {
        self.shared_blks_hit + self.shared_blks_read
    }

    /// Shared buffers per call, or zero for a statement with no calls.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn buffers_per_call(&self) -> f64 {
        if self.calls == 0 {
            0.0
        } else {
            self.total_buffers() as f64 / self.calls as f64
        }
    }
}

/// The statements view, or the reason it could not be read.
#[derive(Debug, Clone, PartialEq)]
pub enum Statements {
    Captured(Vec<StatementStats>),
    /// The extension is not preloaded, not installed, or not readable.
    Unavailable(String),
}

/// One `pg_stat_user_tables` row.
///
/// `n_live_tup` and `n_dead_tup` are gauges. Every other field is a counter.
#[derive(Debug, Clone, PartialEq, Eq, QueryableByName)]
pub struct TableStats {
    #[diesel(sql_type = Text)]
    pub relname: String,
    #[diesel(sql_type = BigInt)]
    pub seq_scan: i64,
    #[diesel(sql_type = BigInt)]
    pub seq_tup_read: i64,
    #[diesel(sql_type = BigInt)]
    pub idx_scan: i64,
    #[diesel(sql_type = BigInt)]
    pub idx_tup_fetch: i64,
    #[diesel(sql_type = BigInt)]
    pub n_tup_ins: i64,
    #[diesel(sql_type = BigInt)]
    pub n_tup_upd: i64,
    #[diesel(sql_type = BigInt)]
    pub n_tup_hot_upd: i64,
    #[diesel(sql_type = BigInt)]
    pub n_tup_del: i64,
    #[diesel(sql_type = BigInt)]
    pub n_live_tup: i64,
    #[diesel(sql_type = BigInt)]
    pub n_dead_tup: i64,
}

impl TableStats {
    /// Dead tuples as a percentage of all tuples, or zero for an empty table.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn dead_pct(&self) -> f64 {
        let all = self.n_live_tup + self.n_dead_tup;
        if all == 0 {
            0.0
        } else {
            100.0 * self.n_dead_tup as f64 / all as f64
        }
    }
}

/// Both views, read from one database at one instant.
#[derive(Debug, Clone)]
pub struct StatsSnapshot {
    pub database: String,
    pub tables: Vec<TableStats>,
    pub statements: Statements,
}

#[derive(QueryableByName)]
struct IntRow {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

#[derive(QueryableByName)]
struct TextRow {
    #[diesel(sql_type = Text)]
    t: String,
}

async fn server_version_num(conn: &mut AsyncPgConnection) -> i64 {
    diesel::sql_query("SELECT current_setting('server_version_num')::bigint AS n")
        .get_result::<IntRow>(conn)
        .await
        .map_or(0, |row| row.n)
}

/// Install the extension and clear the statements of this database only.
///
/// # Errors
/// Returns the server error when the extension is not preloaded or the role
/// cannot reset it.
pub async fn reset_statements(conn: &mut AsyncPgConnection) -> Result<(), String> {
    conn.batch_execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .await
        .map_err(|e| format!("install pg_stat_statements: {e}"))?;
    conn.batch_execute(
        "SELECT pg_stat_statements_reset(0::oid, \
           (SELECT oid FROM pg_database WHERE datname = current_database()), 0::bigint)",
    )
    .await
    .map_err(|e| format!("reset pg_stat_statements for this database: {e}"))
}

/// Wait until no other session uses this database, up to [`QUIESCE_BOUND`].
///
/// Returns `false` when sessions remain at the bound. The snapshot still
/// runs, because partial counters are better than no counters.
async fn quiesce(conn: &mut AsyncPgConnection) -> bool {
    let deadline = Instant::now() + QUIESCE_BOUND;
    loop {
        let others = diesel::sql_query(
            "SELECT COUNT(*) AS n FROM pg_stat_activity \
             WHERE datname = current_database() AND pid <> pg_backend_pid()",
        )
        .get_result::<IntRow>(conn)
        .await
        .map_or(0, |row| row.n);
        if others == 0 {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Make the counters of ended sessions visible to this session.
///
/// `PostgreSQL` 15 and later keep counters in shared memory. An ended session
/// has flushed its own, and `pg_stat_force_next_flush` flushes this one. An
/// older server sends counters to a collector process with a short delay.
async fn flush(conn: &mut AsyncPgConnection) {
    if server_version_num(conn).await >= 150_000 {
        let _ = conn
            .batch_execute("SELECT pg_stat_force_next_flush()")
            .await;
    } else {
        tokio::time::sleep(Duration::from_millis(750)).await;
    }
    let _ = conn.batch_execute("SELECT pg_stat_clear_snapshot()").await;
}

async fn read_tables(conn: &mut AsyncPgConnection) -> Vec<TableStats> {
    diesel::sql_query(
        "SELECT relname::text AS relname, seq_scan, seq_tup_read, \
                COALESCE(idx_scan, 0) AS idx_scan, COALESCE(idx_tup_fetch, 0) AS idx_tup_fetch, \
                n_tup_ins, n_tup_upd, n_tup_hot_upd, n_tup_del, n_live_tup, n_dead_tup \
         FROM pg_stat_user_tables ORDER BY relname",
    )
    .load::<TableStats>(conn)
    .await
    .unwrap_or_else(|e| panic!("read pg_stat_user_tables: {e}"))
}

async fn read_statements(conn: &mut AsyncPgConnection) -> Statements {
    // `total_time` became `total_exec_time` in PostgreSQL 13.
    let time_col = if server_version_num(conn).await >= 130_000 {
        "total_exec_time"
    } else {
        "total_time"
    };
    let sql = format!(
        "SELECT query, calls, rows, {time_col}::float8 AS total_exec_ms, \
                shared_blks_hit, shared_blks_read, shared_blks_dirtied, \
                shared_blks_written, temp_blks_written \
         FROM pg_stat_statements \
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) \
           AND query NOT ILIKE '%pg_stat_statements%' \
           AND query NOT ILIKE '%pg_stat_user_tables%' \
           AND query NOT ILIKE '%pg_stat_activity%'"
    );
    match diesel::sql_query(sql).load::<StatementStats>(conn).await {
        Ok(rows) => Statements::Captured(rows),
        Err(e) => Statements::Unavailable(e.to_string()),
    }
}

/// Read both views from the database `conn` uses.
///
/// The read waits for other sessions to end, then flushes counters. Call it
/// after the workload closes its pools and before the database is dropped.
///
/// # Panics
/// Panics when `pg_stat_user_tables` cannot be read. That view exists on every
/// supported server, so a failure is a real fault.
pub async fn capture(conn: &mut AsyncPgConnection) -> StatsSnapshot {
    let quiet = quiesce(conn).await;
    if !quiet {
        eprintln!("warning: other sessions still use the database; the counters can be partial");
    }
    flush(conn).await;
    let database = diesel::sql_query("SELECT current_database()::text AS t")
        .get_result::<TextRow>(conn)
        .await
        .map_or_else(|_| String::new(), |row| row.t);
    let tables = read_tables(conn).await;
    let statements = read_statements(conn).await;
    StatsSnapshot {
        database,
        tables,
        statements,
    }
}

/// Connect to `url`, capture both views, and write them to `dir`.
///
/// The e2e bench calls this from its teardown, before the drop.
///
/// # Errors
/// Returns an error when the connection or a file write fails.
pub async fn snapshot_to_dir(url: &str, dir: &Path, label: &str) -> Result<Vec<PathBuf>, String> {
    let mut conn = AsyncPgConnection::establish(url)
        .await
        .map_err(|e| format!("connect for the stats snapshot: {e}"))?;
    let snapshot = capture(&mut conn).await;
    drop(conn);
    write_snapshot(dir, label, &snapshot, 25).map_err(|e| format!("write the stats snapshot: {e}"))
}

/// Write `{label}-pg_stat_statements.txt` and `{label}-pg_stat_user_tables.txt`.
///
/// # Errors
/// Returns the I/O error of the first write that fails.
pub fn write_snapshot(
    dir: &Path,
    label: &str,
    snapshot: &StatsSnapshot,
    top: usize,
) -> std::io::Result<Vec<PathBuf>> {
    std::fs::create_dir_all(dir)?;
    let statements = dir.join(format!("{label}-pg_stat_statements.txt"));
    let tables = dir.join(format!("{label}-pg_stat_user_tables.txt"));
    std::fs::write(
        &statements,
        format!(
            "-- pg_stat_statements, dbid of {} only, top {top} by shared buffers --\n{}",
            snapshot.database,
            render_statements(&snapshot.statements, top)
        ),
    )?;
    std::fs::write(
        &tables,
        format!(
            "-- pg_stat_user_tables of {} --\n{}",
            snapshot.database,
            render_tables(&snapshot.tables)
        ),
    )?;
    Ok(vec![statements, tables])
}

/// One line of SQL text, cut to `max` characters.
fn one_line(query: &str, max: usize) -> String {
    let flat = query.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= max {
        flat
    } else {
        let cut: String = flat.chars().take(max).collect();
        format!("{cut}…")
    }
}

/// Render the top `top` statements by shared buffers as a Markdown table.
///
/// The share column divides by the total of every captured statement, not
/// only of the rows shown.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn render_statements(statements: &Statements, top: usize) -> String {
    let rows = match statements {
        Statements::Unavailable(reason) => {
            return format!("pg_stat_statements unavailable: {reason}\n");
        }
        Statements::Captured(rows) => rows,
    };
    let mut sorted: Vec<&StatementStats> = rows.iter().collect();
    sorted.sort_by(|a, b| {
        b.total_buffers()
            .cmp(&a.total_buffers())
            .then_with(|| a.query.cmp(&b.query))
    });
    let total: i64 = rows.iter().map(StatementStats::total_buffers).sum();
    let mut out = String::from(
        "| % buffers | calls | buffers | buffers/call | rows | total ms | temp blks written | statement |\n\
         |--:|--:|--:|--:|--:|--:|--:|--|\n",
    );
    for row in sorted.into_iter().take(top) {
        let share = if total == 0 {
            0.0
        } else {
            100.0 * row.total_buffers() as f64 / total as f64
        };
        let _ = writeln!(
            out,
            "| {share:.1} | {} | {} | {:.1} | {} | {:.1} | {} | `{}` |",
            row.calls,
            row.total_buffers(),
            row.buffers_per_call(),
            row.rows,
            row.total_exec_ms,
            row.temp_blks_written,
            one_line(&row.query, 160).replace('|', "\\|"),
        );
    }
    let _ = writeln!(
        out,
        "\n{} statements, {total} shared buffers in all.",
        rows.len()
    );
    out
}

/// Render table stats as a Markdown table, most `seq_tup_read` first.
#[must_use]
pub fn render_tables(tables: &[TableStats]) -> String {
    let mut sorted: Vec<&TableStats> = tables.iter().collect();
    sorted.sort_by(|a, b| {
        b.seq_tup_read
            .cmp(&a.seq_tup_read)
            .then_with(|| a.relname.cmp(&b.relname))
    });
    let mut out = String::from(
        "| relname | seq_scan | seq_tup_read | idx_scan | idx_tup_fetch | n_tup_ins | n_tup_upd | \
         n_tup_hot_upd | n_tup_del | n_live_tup | n_dead_tup | dead_pct |\n\
         |--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|\n",
    );
    for t in sorted {
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {:.1} |",
            t.relname,
            t.seq_scan,
            t.seq_tup_read,
            t.idx_scan,
            t.idx_tup_fetch,
            t.n_tup_ins,
            t.n_tup_upd,
            t.n_tup_hot_upd,
            t.n_tup_del,
            t.n_live_tup,
            t.n_dead_tup,
            t.dead_pct(),
        );
    }
    out
}

/// The change in each table's counters from `before` to `after`.
///
/// A counter is a difference, floored at zero for a reset between the two
/// reads. A gauge keeps its `after` value. A table absent from `before`
/// keeps its full `after` counters.
#[must_use]
pub fn table_deltas(before: &[TableStats], after: &[TableStats]) -> Vec<TableStats> {
    after
        .iter()
        .map(|a| {
            let Some(b) = before.iter().find(|b| b.relname == a.relname) else {
                return a.clone();
            };
            let d = |x: i64, y: i64| (x - y).max(0);
            TableStats {
                relname: a.relname.clone(),
                seq_scan: d(a.seq_scan, b.seq_scan),
                seq_tup_read: d(a.seq_tup_read, b.seq_tup_read),
                idx_scan: d(a.idx_scan, b.idx_scan),
                idx_tup_fetch: d(a.idx_tup_fetch, b.idx_tup_fetch),
                n_tup_ins: d(a.n_tup_ins, b.n_tup_ins),
                n_tup_upd: d(a.n_tup_upd, b.n_tup_upd),
                n_tup_hot_upd: d(a.n_tup_hot_upd, b.n_tup_hot_upd),
                n_tup_del: d(a.n_tup_del, b.n_tup_del),
                n_live_tup: a.n_live_tup,
                n_dead_tup: a.n_dead_tup,
            }
        })
        .collect()
}
