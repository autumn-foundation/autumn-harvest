//! Snapshot `pg_stat_statements` and `pg_stat_user_tables` (issue #1956).
//!
//! `pg_stat_user_tables` lives in the database it describes. A harness that
//! drops its database at exit loses those counters. So a harness calls
//! [`capture`] first and drops the database after it.
//!
//! `pg_stat_statements` is cluster-wide. Every read here filters on the dbid
//! of the current database. Call [`reset_statements`] before the workload.
//! The view keeps the rows of a dropped database, and after OID wraparound a
//! new database can get that OID.

// `benches/e2e_bench.rs` includes this file and uses only part of it.
#![allow(dead_code)]

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Double, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};

/// How long [`capture`] waits for other sessions to flush their counters.
///
/// A session flushes its pending table counters when it ends. An idle session
/// flushes them within [`IDLE_FLUSH`]. The bound covers that interval with a
/// margin.
const QUIESCE_BOUND: Duration = Duration::from_secs(15);

/// The wall-clock bound on one [`snapshot_to_dir`] call: the quiesce bound
/// plus time to connect, read and write.
pub const SNAPSHOT_BOUND: Duration = Duration::from_secs(45);

/// Statements a rendered table shows.
pub const TOP_STATEMENTS: usize = 25;

/// The first server version that keeps counters in shared memory.
const SHARED_MEMORY_STATS: i64 = 150_000;

/// The longest time an idle session keeps unflushed counters.
///
/// `PostgreSQL` 15 and later flush an idle session within
/// `PGSTAT_IDLE_INTERVAL`, which is 10 seconds. A session idle for longer has
/// no counters that the snapshot can miss.
const IDLE_FLUSH: &str = "10 seconds";

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
/// `n_live_tup` and `n_dead_tup` are gauges. The other numeric fields are
/// counters.
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

/// Both views, read from one database after the workload.
#[derive(Debug, Clone)]
pub struct StatsSnapshot {
    pub database: String,
    pub tables: Vec<TableStats>,
    pub statements: Statements,
    /// Sessions that had not flushed their counters at the read, or the error
    /// that hid them. A non-empty list marks a partial snapshot.
    pub lingering: Vec<String>,
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

/// What [`reset_counters`] could not reset. `None` means that reset worked.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResetOutcome {
    /// The error of the `pg_stat_reset` call, if any.
    pub tables: Option<String>,
    /// The error of the `pg_stat_statements` reset, if any.
    pub statements: Option<String>,
}

impl ResetOutcome {
    /// Whether both views were reset.
    #[must_use]
    pub const fn is_clean(&self) -> bool {
        self.tables.is_none() && self.statements.is_none()
    }
}

/// Clear both views for this database, so a later snapshot holds only what
/// runs after this call. Each view is reset on its own, so a server without
/// `pg_stat_statements` still gets clean table counters.
pub async fn reset_counters(conn: &mut AsyncPgConnection) -> ResetOutcome {
    flush_counters(conn).await;
    let tables = conn
        .batch_execute("SELECT pg_stat_reset()")
        .await
        .err()
        .map(|e| format!("reset the table counters of this database: {e}"));
    let statements = reset_statements(conn).await.err();
    ResetOutcome { tables, statements }
}

/// What a teardown does with a shard snapshot, given its setup reset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotPlan {
    /// The table counters hold the setup too, so no file is written.
    Refuse(String),
    /// Write both files. A note replaces the statements when their reset
    /// failed, because the view then holds the setup too.
    Write { statements_note: Option<String> },
}

/// The [`SnapshotPlan`] for a shard whose setup reset gave `reset`.
#[must_use]
pub fn snapshot_plan(reset: &ResetOutcome) -> SnapshotPlan {
    if let Some(e) = &reset.tables {
        return SnapshotPlan::Refuse(format!(
            "the setup reset failed, so the table counters hold the setup too: {e}"
        ));
    }
    SnapshotPlan::Write {
        statements_note: reset.statements.as_ref().map(|e| {
            format!(
                "not captured: the setup reset of pg_stat_statements failed, so the view \
                 holds the setup too: {e}"
            )
        }),
    }
}

/// Remove the snapshot pair of `label` from `dir`, if it is there.
pub fn remove_snapshot(dir: &Path, label: &str) {
    for path in snapshot_paths(dir, label) {
        let _ = std::fs::remove_file(path);
    }
}

/// The statements file and the tables file of `label` in `dir`.
fn snapshot_paths(dir: &Path, label: &str) -> [PathBuf; 2] {
    [
        dir.join(format!("{label}-pg_stat_statements.txt")),
        dir.join(format!("{label}-pg_stat_user_tables.txt")),
    ]
}

/// Wait until every other session on this database has flushed its counters,
/// up to [`QUIESCE_BOUND`].
///
/// A session has flushed when it has ended. On `PostgreSQL` 15 and later, a
/// session idle for longer than [`IDLE_FLUSH`] has flushed too. An older
/// server has no idle flush, so there only an ended session counts.
///
/// Returns the sessions that have not flushed at the bound. The snapshot
/// still runs then, because partial counters are better than no counters.
async fn quiesce(conn: &mut AsyncPgConnection) -> Vec<String> {
    let deadline = Instant::now() + QUIESCE_BOUND;
    let idle_rule = if server_version_num(conn).await >= SHARED_MEMORY_STATS {
        format!("AND NOT (state = 'idle' AND state_change < now() - INTERVAL '{IDLE_FLUSH}')")
    } else {
        String::new()
    };
    let sql = format!(
        "SELECT format('pid %s, application %L, state %s for %ss, query %L', \
                pid, application_name, state, \
                round(extract(epoch FROM now() - state_change)), left(query, 80)) AS t \
         FROM pg_stat_activity \
         WHERE datname = current_database() AND pid <> pg_backend_pid() {idle_rule} \
         ORDER BY pid"
    );
    loop {
        let others = diesel::sql_query(&sql)
            .load::<TextRow>(conn)
            .await
            .map_or_else(
                |e| vec![format!("cannot list sessions: {e}")],
                |rows| rows.into_iter().map(|row| row.t).collect(),
            );
        if others.is_empty() || Instant::now() >= deadline {
            return others;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Make the counters of this session and of ended sessions visible here.
///
/// `PostgreSQL` 15 and later keep counters in shared memory. An ended session
/// has flushed its own. `pg_stat_force_next_flush` makes the next report of
/// this session flush its own, and the next statement gives that report.
///
/// An older server sends counters to a collector process at most every
/// 500 ms. So this waits, runs one statement to send them, and waits again.
pub async fn flush_counters(conn: &mut AsyncPgConnection) {
    if server_version_num(conn).await >= SHARED_MEMORY_STATS {
        let _ = conn
            .batch_execute("SELECT pg_stat_force_next_flush()")
            .await;
    } else {
        tokio::time::sleep(Duration::from_millis(600)).await;
        let _ = conn.batch_execute("SELECT 1").await;
        tokio::time::sleep(Duration::from_millis(600)).await;
    }
    let _ = conn.batch_execute("SELECT pg_stat_clear_snapshot()").await;
}

/// Flush this session's counters, then read `pg_stat_user_tables`.
///
/// # Errors
/// Returns the server error when the view cannot be read.
pub async fn read_table_stats(conn: &mut AsyncPgConnection) -> Result<Vec<TableStats>, String> {
    flush_counters(conn).await;
    read_tables(conn).await
}

async fn read_tables(conn: &mut AsyncPgConnection) -> Result<Vec<TableStats>, String> {
    diesel::sql_query(
        "SELECT relname::text AS relname, seq_scan, seq_tup_read, \
                COALESCE(idx_scan, 0) AS idx_scan, COALESCE(idx_tup_fetch, 0) AS idx_tup_fetch, \
                n_tup_ins, n_tup_upd, n_tup_hot_upd, n_tup_del, n_live_tup, n_dead_tup \
         FROM pg_stat_user_tables ORDER BY relname",
    )
    .load::<TableStats>(conn)
    .await
    .map_err(|e| format!("read pg_stat_user_tables: {e}"))
}

async fn read_statements(conn: &mut AsyncPgConnection) -> Statements {
    // The view exists only where the extension is installed. A failure here
    // shows up as the read error below.
    let _ = conn
        .batch_execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .await;
    // `total_time` became `total_exec_time` in PostgreSQL 13.
    let time_col = if server_version_num(conn).await >= 130_000 {
        "total_exec_time"
    } else {
        "total_time"
    };
    let sql = format!(
        "SELECT query, calls, rows, {time_col}::float8 AS total_exec_ms, \
                shared_blks_hit, shared_blks_read, temp_blks_written \
         FROM pg_stat_statements \
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) \
           AND query NOT ILIKE '%pg_stat_statements%' \
           AND query NOT ILIKE '%pg_stat_user_tables%' \
           AND query NOT ILIKE '%pg_stat_activity%' \
           AND query NOT ILIKE '%pg_stat_force_next_flush%' \
           AND query NOT ILIKE '%pg_stat_clear_snapshot%' \
           AND query NOT ILIKE '%server_version_num%' \
           AND query <> 'SELECT current_database()::text AS t'"
    );
    match diesel::sql_query(sql).load::<StatementStats>(conn).await {
        Ok(rows) => Statements::Captured(rows),
        Err(e) => Statements::Unavailable(e.to_string()),
    }
}

/// Read both views from the database `conn` uses.
///
/// The read waits for other sessions to flush their counters. Call it after
/// the workload closes its pools and before the harness drops the database.
///
/// # Errors
/// Returns an error when `pg_stat_user_tables` cannot be read. A missing
/// statements view is not an error: [`Statements::Unavailable`] records it.
pub async fn try_capture(conn: &mut AsyncPgConnection) -> Result<StatsSnapshot, String> {
    let lingering = quiesce(conn).await;
    if !lingering.is_empty() {
        eprintln!(
            "warning: {} session(s) had not flushed their counters, so the snapshot can be \
             partial: {lingering:?}",
            lingering.len()
        );
    }
    flush_counters(conn).await;
    let database = diesel::sql_query("SELECT current_database()::text AS t")
        .get_result::<TextRow>(conn)
        .await
        .map_or_else(|_| String::new(), |row| row.t);
    let tables = read_tables(conn).await?;
    let statements = read_statements(conn).await;
    Ok(StatsSnapshot {
        database,
        tables,
        statements,
        lingering,
    })
}

/// [`try_capture`] for a harness that cannot go on without the snapshot.
///
/// # Panics
/// Panics when `pg_stat_user_tables` cannot be read.
pub async fn capture(conn: &mut AsyncPgConnection) -> StatsSnapshot {
    try_capture(conn).await.unwrap_or_else(|e| panic!("{e}"))
}

/// Connect to `url`, capture both views, and write them to `dir`.
///
/// It never panics, so a failed snapshot cannot skip a drop after it.
///
/// # Errors
/// Returns an error when the connection, the read or a file write fails.
pub async fn snapshot_to_dir(url: &str, dir: &Path, label: &str) -> Result<Vec<PathBuf>, String> {
    let mut conn = AsyncPgConnection::establish(url)
        .await
        .map_err(|e| format!("connect for the stats snapshot: {e}"))?;
    snapshot_conn_to_dir(&mut conn, dir, label, None).await
}

/// Capture both views on `conn` and write them to `dir`.
///
/// The e2e bench calls this on each shard's lease, before the drop. A
/// session flushes its own counters, so the lease needs no wait.
///
/// A `statements_note` replaces the statements, for a view whose setup reset
/// failed.
///
/// # Errors
/// Returns an error when the read or a file write fails.
pub async fn snapshot_conn_to_dir(
    conn: &mut AsyncPgConnection,
    dir: &Path,
    label: &str,
    statements_note: Option<&str>,
) -> Result<Vec<PathBuf>, String> {
    let mut snapshot = try_capture(conn).await?;
    if let Some(note) = statements_note {
        snapshot.statements = Statements::Unavailable(note.to_string());
    }
    write_snapshot(dir, label, &snapshot, TOP_STATEMENTS)
        .map_err(|e| format!("write the stats snapshot: {e}"))
}

/// A `-- PARTIAL --` line for a snapshot with unflushed sessions, or nothing.
#[must_use]
pub fn partial_banner(snapshot: &StatsSnapshot) -> String {
    if snapshot.lingering.is_empty() {
        String::new()
    } else {
        format!(
            "-- PARTIAL: {} session(s) had not flushed their counters: {:?} --\n",
            snapshot.lingering.len(),
            snapshot.lingering
        )
    }
}

/// Write `{label}-pg_stat_statements.txt` and `{label}-pg_stat_user_tables.txt`.
///
/// Both files are staged as `.tmp` files and then renamed. On an error, the
/// staged files and both targets are removed. So `dir` never keeps a pair
/// from an older run next to a file from this run.
///
/// # Errors
/// Returns the I/O error of the first step that fails.
pub fn write_snapshot(
    dir: &Path,
    label: &str,
    snapshot: &StatsSnapshot,
    top: usize,
) -> std::io::Result<Vec<PathBuf>> {
    std::fs::create_dir_all(dir)?;
    let partial = partial_banner(snapshot);
    let [statements, tables] = snapshot_paths(dir, label);
    let bodies = [
        format!(
            "-- pg_stat_statements, dbid of {} only, since its last reset, top {top} by shared buffers --\n{partial}{}",
            snapshot.database,
            render_statements(&snapshot.statements, top)
        ),
        format!(
            "-- pg_stat_user_tables of {} --\n{partial}{}",
            snapshot.database,
            render_tables(&snapshot.tables)
        ),
    ];
    let targets = [statements, tables];
    let staged = targets.clone().map(|p| p.with_extension("txt.tmp"));
    let result = staged
        .iter()
        .zip(&bodies)
        .try_for_each(|(path, body)| std::fs::write(path, body))
        .and_then(|()| {
            staged
                .iter()
                .zip(&targets)
                .try_for_each(|(from, to)| std::fs::rename(from, to))
        });
    if let Err(e) = result {
        for path in staged.iter().chain(&targets) {
            let _ = std::fs::remove_file(path);
        }
        return Err(e);
    }
    Ok(targets.to_vec())
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
