//! `cargo xtask perf external-outbox-scan-matrix`: the port of
//! `autumn-harvest/scripts/external_outbox_scan_matrix.sh`.
//!
//! The four-way comparison harness for the external-outbox claim queries
//! (issue #1486). `docs/performance-external-outbox-scan.md` documents it.
//!
//! The Rust evidence capture, `cargo xtask perf external-outbox-scan`,
//! measures one thing well. It runs the shipped query against the shipped
//! schema, through the real drain loop. It cannot say what the other three
//! combinations would have cost, because it runs the query that the crate
//! actually ships.
//!
//! This harness answers that. It swaps schema and query text freely over one
//! fixture. The four-way table and the stale-estimate table of the page need
//! exactly that. Every figure on that page that does not come from
//! `docs/perf-artifacts/external-outbox-scan/` comes from here.
//!
//! Usage:
//!
//! ```text
//! PGURL=postgres://postgres:postgres@localhost:5432 \
//!   cargo xtask perf external-outbox-scan-matrix
//! ```
//!
//! Preconditions: `psql` on `PATH`, a Postgres 16 that the URL can reach as a
//! superuser, and `pg_stat_statements` in `shared_preload_libraries`. The
//! harness creates and drops its own databases. It never touches an existing
//! one.
//!
//! Per scenario, it prints the total buffers over a full 50-request drain and
//! the buffer count of the cold single-claim plan. The drain total comes from
//! `pg_stat_statements`, scoped to the own database of this run.
//!
//! Every scenario builds its own 1.02M-row database from scratch, so each one
//! costs about three minutes. The default run is the two tables that the page
//! leads with, and it takes roughly 25 minutes. The request-history sweep
//! triples that, so it is opt-in:
//!
//! ```text
//! OUTBOX_MATRIX_HISTORY=1 cargo xtask perf external-outbox-scan-matrix
//! ```

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, ExitStatus, Stdio};

use regex::Regex;
use smop::prelude::*;

use super::support::{self, Ctx};

/// Issue #1486's own fixture.
///
/// It holds 5,000 RUNNING executions with 200 events each, and a tail of
/// 2,000 terminal executions with 10 events each. It also holds 50
/// unresolved requests.
const FIXTURE_SQL: &str = r"INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, started_at, created_at)
SELECT gen_random_uuid(), 'probe_wf', 'probe_wf_'||gs, gen_random_uuid(), 0, 'RUNNING', '{}'::jsonb, 'default', NOW(), NOW()
FROM generate_series(1,5000) gs;
INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, started_at, created_at)
SELECT gen_random_uuid(), 'probe_wf_done', 'probe_wf_done_'||gs, gen_random_uuid(), 0, 'COMPLETED', '{}'::jsonb, 'default', NOW(), NOW()
FROM generate_series(1,2000) gs;
INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
SELECT e.id, gs,
       (ARRAY['ActivityScheduled','ActivityStarted','ActivityCompleted','WorkflowTaskScheduled'])[1 + (gs % 4)],
       jsonb_build_object('type', 'ActivityScheduled', 'data', jsonb_build_object('activity_id', 'a'||gs)),
       NOW() - (gs || ' seconds')::interval
FROM harvest_workflow_executions e, generate_series(1,200) gs
WHERE e.state = 'RUNNING';
INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
SELECT e.id, gs,
       (ARRAY['ActivityScheduled','ActivityStarted','ActivityCompleted','WorkflowTaskScheduled'])[1 + (gs % 4)],
       jsonb_build_object('type', 'ActivityScheduled', 'data', jsonb_build_object('activity_id', 'a'||gs)),
       NOW() - (gs || ' seconds')::interval
FROM harvest_workflow_executions e, generate_series(1,10) gs
WHERE e.state <> 'RUNNING';
INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
SELECT e.id, 5000, 'ExternalSignalRequested',
       jsonb_build_object('type','ExternalSignalRequested','data',
           jsonb_build_object('signal_id', 'sig-'||e.workflow_id)),
       NOW()
FROM (SELECT * FROM harvest_workflow_executions WHERE state = 'RUNNING' ORDER BY workflow_id LIMIT 50) e;
";

/// The pre-fix claim query.
const LEGACY_SQL: &str = r"SELECT e.* FROM harvest_events e
INNER JOIN harvest_workflow_executions execs ON e.workflow_exec_id = execs.id
WHERE e.event_type = 'ExternalSignalRequested'
  AND execs.state = 'RUNNING'
  AND execs.shard_id = ANY('{0}'::int[])
  AND (e.event_data->'data'->>'signal_id') IS NOT NULL
  AND NOT (e.id = ANY('{}'::bigint[]))
  AND NOT EXISTS (
      SELECT 1 FROM harvest_events res
      WHERE res.workflow_exec_id = e.workflow_exec_id
        AND res.event_type IN ('ExternalSignalDelivered', 'ExternalSignalFailed')
        AND res.event_data->'data'->>'signal_id' = e.event_data->'data'->>'signal_id'
  )
LIMIT 1
FOR UPDATE OF e SKIP LOCKED
";

/// The rewritten claim query, as shipped.
const REWRITTEN_SQL: &str = r"SELECT e.* FROM harvest_events e
JOIN LATERAL (
    SELECT 1 AS running_exec FROM harvest_workflow_executions x
    WHERE x.id = e.workflow_exec_id
      AND x.state = 'RUNNING'
      AND x.shard_id = ANY('{0}'::int[])
    LIMIT 1
) running ON TRUE
WHERE e.event_type = 'ExternalSignalRequested'
  AND (e.event_data->'data'->>'signal_id') IS NOT NULL
  AND NOT (e.id = ANY('{}'::bigint[]))
  AND NOT EXISTS (
      SELECT 1 FROM harvest_events res
      WHERE res.workflow_exec_id = e.workflow_exec_id
        AND res.event_type IN ('ExternalSignalDelivered', 'ExternalSignalFailed')
        AND res.event_data->'data'->>'signal_id' = e.event_data->'data'->>'signal_id'
  )
ORDER BY e.timestamp, e.id
LIMIT 1
FOR UPDATE OF e SKIP LOCKED
";

/// The shipped query with the executions join as a plain `INNER JOIN`.
///
/// It replaces the `LATERAL`. Everything else is identical: the `ORDER BY`
/// pin and the `NOT EXISTS` resolution check. So a comparison against
/// [`REWRITTEN_SQL`] isolates what the pin of that one join is worth.
const REWRITTEN_PLAIN_JOIN_SQL: &str = r"SELECT e.* FROM harvest_events e
INNER JOIN harvest_workflow_executions execs ON e.workflow_exec_id = execs.id
WHERE e.event_type = 'ExternalSignalRequested'
  AND execs.state = 'RUNNING'
  AND execs.shard_id = ANY('{0}'::int[])
  AND (e.event_data->'data'->>'signal_id') IS NOT NULL
  AND NOT (e.id = ANY('{}'::bigint[]))
  AND NOT EXISTS (
      SELECT 1 FROM harvest_events res
      WHERE res.workflow_exec_id = e.workflow_exec_id
        AND res.event_type IN ('ExternalSignalDelivered', 'ExternalSignalFailed')
        AND res.event_data->'data'->>'signal_id' = e.event_data->'data'->>'signal_id'
  )
ORDER BY e.timestamp, e.id
LIMIT 1
FOR UPDATE OF e SKIP LOCKED
";

/// The pending index alone, without the guard of the migration.
///
/// The indexes come without that guard, so a scenario can install a subset.
const IDX_PENDING_SQL: &str = r"CREATE INDEX idx_harvest_events_external_outbox_pending
    ON harvest_events (event_type, timestamp, id)
    WHERE event_type IN ('ExternalSignalRequested', 'ExternalCancelRequested', 'ExternalAwaitRequested');
";

/// Both indexes, without the guard of the migration.
const IDX_ALL_SQL: &str = r"CREATE INDEX idx_harvest_events_external_outbox_pending
    ON harvest_events (event_type, timestamp, id)
    WHERE event_type IN ('ExternalSignalRequested', 'ExternalCancelRequested', 'ExternalAwaitRequested');
CREATE INDEX idx_harvest_events_external_signal_resolved
    ON harvest_events (workflow_exec_id, (event_data->'data'->>'signal_id'))
    WHERE event_type IN ('ExternalSignalDelivered', 'ExternalSignalFailed');
";

/// A row estimate left far above the truth.
///
/// A drained outage backlog leaves this condition behind. `ANALYZE` records
/// the backlog, the backlog drains, and the stored estimate stays far above
/// the truth. The query rewrite exists for this case.
const STALE_SQL: &str = r"INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
SELECT e.id, 20000 + gs, 'ExternalSignalRequested',
       jsonb_build_object('type','ExternalSignalRequested','data',
           jsonb_build_object('signal_id','burst-'||e.workflow_id||'-'||gs)),
       NOW()
FROM harvest_workflow_executions e, generate_series(1,4) gs
WHERE e.state = 'RUNNING';
ANALYZE harvest_events;
ANALYZE harvest_workflow_executions;
DELETE FROM harvest_events
WHERE event_type = 'ExternalSignalRequested'
  AND event_data->'data'->>'signal_id' LIKE 'burst-%';
";

/// The migration that the schema bundle includes and each scenario undoes.
const OUTBOX_MIGRATION_DOWN: &str =
    "autumn-harvest/migrations/20260911213344_harvest_external_outbox_scan_indexes/down.sql";

/// Returns the lifetime request history fixture with `h` resolved requests.
///
/// `harvest_events` is append-only, so the pending index holds every request
/// that the deployment ever made. Each claim walks the resolved ones before it
/// reaches a pending one. So the claim cost tracks lifetime requests, not the
/// backlog.
pub fn history_sql(h: u32) -> String {
    let per = h / 1000;
    format!(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
SELECT e.id, 800000 + gs, 'ExternalSignalRequested',
       jsonb_build_object('type','ExternalSignalRequested','data',
           jsonb_build_object('signal_id','h-'||e.workflow_id||'-'||gs)),
       NOW() - interval '30 days' + (gs || ' seconds')::interval
FROM (SELECT * FROM harvest_workflow_executions WHERE state='RUNNING' ORDER BY workflow_id LIMIT 1000) e,
     generate_series(1, {per}) gs;
INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
SELECT e.id, 850000 + gs, 'ExternalSignalDelivered',
       jsonb_build_object('type','ExternalSignalDelivered','data',
           jsonb_build_object('signal_id','h-'||e.workflow_id||'-'||gs)),
       NOW() - interval '29 days'
FROM (SELECT * FROM harvest_workflow_executions WHERE state='RUNNING' ORDER BY workflow_id LIMIT 1000) e,
     generate_series(1, {per}) gs;
"
    )
}

/// Concatenates every migration in directory order, as a fresh database is built.
///
/// Each directory gets a `-- ==== <dir>/` header line. A directory without an
/// `up.sql` contributes only that header and a blank line.
pub fn schema_sql(migrations: &Path) -> Result<String> {
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(migrations)
        .with_context(|| format!("listing {}", migrations.display()))?
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        // A shell glob skips hidden entries, and `*/` keeps directories only.
        .filter(|path| {
            path.is_dir()
                && !path
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with('.'))
        })
        .collect();
    dirs.sort();
    let mut schema = String::new();
    for dir in dirs {
        let _ = writeln!(schema, "-- ==== {}/", dir.display());
        if let Ok(up) = std::fs::read_to_string(dir.join("up.sql")) {
            schema.push_str(&up);
        }
        schema.push('\n');
    }
    Ok(schema)
}

/// Generates the drain: 50 claim-and-resolve rounds, then one claim that finds none.
pub fn drain_sql(query: &str) -> String {
    // The shell read the query through `$(cat file)`, which drops the
    // trailing newlines.
    let claim = query.trim_end_matches('\n');
    // Project the two columns that the resolution marker needs, so `\gset`
    // can carry them into the INSERT. The predicate is untouched.
    let projected = claim.strip_prefix("SELECT e.*").map_or_else(
        || claim.to_string(),
        |rest| {
            format!(
                "SELECT e.workflow_exec_id AS cexec, e.event_data->'data'->>'signal_id' AS csig{rest}"
            )
        },
    );
    let mut out = String::from(
        "SELECT pg_stat_statements_reset(0, (SELECT oid FROM pg_database WHERE datname = current_database()), 0);\n",
    );
    for i in 1..=50 {
        out.push_str("BEGIN;\n");
        out.push_str(&projected);
        out.push('\n');
        out.push_str("\\gset\n");
        let _ = writeln!(
            out,
            "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp) VALUES (:'cexec', 900000 + {i}, 'ExternalSignalDelivered', jsonb_build_object('type','ExternalSignalDelivered','data',jsonb_build_object('signal_id', :'csig')), NOW());"
        );
        out.push_str("COMMIT;\n");
    }
    out.push_str("BEGIN;\n");
    out.push_str(claim);
    out.push_str("\n;\nCOMMIT;\n");
    out.push_str("SELECT sum(shared_blks_hit + shared_blks_read) AS total_buffers, sum(calls) AS statements\n");
    out.push_str("FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database());\n");
    out
}

/// Returns the first `shared hit=N( read=N)?` of a plan, as `grep -oE ... | head -1` did.
pub fn cold_buffers(plan: &str) -> Option<String> {
    let regex = Regex::new(r"shared hit=[0-9]+( read=[0-9]+)?").ok()?;
    regex.find(plan).map(|found| found.as_str().to_string())
}

/// Returns the first `|` field of the last line, as `tail -1 | cut -d'|' -f1` did.
pub fn drain_total(drain: &str) -> String {
    let last = drain
        .trim_end_matches('\n')
        .rsplit('\n')
        .next()
        .unwrap_or("");
    last.split('|').next().unwrap_or("").to_string()
}

/// Formats one result line, as `printf '%-44s drain=%-12s cold claim: %s\n'` did.
pub fn result_line(name: &str, total: &str, cold: &str) -> String {
    format!("{name:<44} drain={total:<12} cold claim: {cold}")
}

/// The fixed inputs of every scenario.
struct Matrix {
    pgurl: String,
    root: PathBuf,
    work: PathBuf,
    db: String,
}

/// How a scenario ended.
enum Outcome {
    /// The scenario printed its line.
    Reported,
    /// The scenario stopped. The command exits with this code.
    Stopped(ExitCode),
}

impl Matrix {
    fn url(&self, db: &str) -> String {
        format!("{}/{db}", self.pgurl)
    }

    /// Returns `psql "$PGURL/$1" -v ON_ERROR_STOP=1 -q <args>`.
    fn psql_run(&self, args: &[&str]) -> Command {
        let mut command = Command::new("psql");
        command
            .arg(self.url(&self.db))
            .args(["-v", "ON_ERROR_STOP=1", "-q"])
            .args(args);
        command
    }

    // `ON_ERROR_STOP` here too, and deliberately so. `psql -f` otherwise
    // continues past a failed statement. The drain would then still print a
    // `pg_stat_statements` total that covers only the steps that ran. A
    // partial total looks the same as a real one once it reaches the page.
    // So this harness stops instead of publishing it.
    /// Returns `psql "$PGURL/$1" -At -v ON_ERROR_STOP=1 <args>`.
    fn psql_at(&self, args: &[&str]) -> Command {
        let mut command = Command::new("psql");
        command
            .arg(self.url(&self.db))
            .args(["-At", "-v", "ON_ERROR_STOP=1"])
            .args(args);
        command
    }

    /// Returns `psql "$PGURL/postgres" -q -c <sql>`.
    fn psql_admin(&self, sql: &str) -> Command {
        let mut command = Command::new("psql");
        command.arg(self.url("postgres")).args(["-q", "-c", sql]);
        command
    }

    fn work_file(&self, name: &str) -> String {
        self.work.join(name).display().to_string()
    }

    /// Runs one `>/dev/null` step, and stops the scenario when it fails.
    fn quiet(command: &mut Command, also_stderr: bool) -> Result<Option<ExitCode>> {
        if also_stderr {
            command.stderr(Stdio::null());
        }
        let status: ExitStatus = support::status_to(command, Stdio::null())?;
        Ok((!status.success()).then(|| support::exit_code(status)))
    }

    /// `run_scenario <label> <query file> [index file] [extra fixture file]`.
    ///
    /// The files are names inside the scratch directory.
    fn run_scenario(
        &self,
        name: &str,
        query: &str,
        indexes: Option<&str>,
        extra: Option<&str>,
    ) -> Result<Outcome> {
        let drop_db = format!("DROP DATABASE IF EXISTS {};", self.db);
        let create_db = format!("CREATE DATABASE {};", self.db);
        let schema = self.work_file("schema.sql");
        let down = self.root.join(OUTBOX_MIGRATION_DOWN).display().to_string();
        let fixture = self.work_file("fixture.sql");
        let query = self.work_file(query);
        let indexes = indexes.map(|name| self.work_file(name));
        let extra = extra.map(|name| self.work_file(name));

        let mut steps: Vec<(Command, bool)> = vec![
            (self.psql_admin(&drop_db), false),
            (self.psql_admin(&create_db), false),
            (self.psql_run(&["-f", &schema]), true),
            // The bundle above includes the own migration of this change. So
            // every scenario starts from no indexes and installs only what it
            // asks for. Without this, the baseline silently measures the fixed
            // state.
            (self.psql_run(&["-f", &down]), false),
            (
                self.psql_run(&["-c", "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"]),
                false,
            ),
            (self.psql_run(&["-f", &fixture]), false),
        ];
        if let Some(indexes) = &indexes {
            steps.push((self.psql_run(&["-f", indexes]), false));
        }
        steps.push((
            self.psql_run(&[
                "-c",
                "ANALYZE harvest_events; ANALYZE harvest_workflow_executions;",
            ]),
            false,
        ));
        // After the ANALYZE, so a scenario can leave the statistics stale on
        // purpose, or add history that the statistics already describe.
        if let Some(extra) = &extra {
            steps.push((self.psql_run(&["-f", extra]), false));
        }
        for (mut command, also_stderr) in steps {
            if let Some(code) = Self::quiet(&mut command, also_stderr)? {
                return Ok(Outcome::Stopped(code));
            }
        }

        let query_text = fs::read_string(&query)?;
        let explain = format!(
            "BEGIN; EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) {}; ROLLBACK;",
            query_text.trim_end_matches('\n')
        );
        let (status, plan) = support::capture(&mut self.psql_at(&["-c", &explain]))?;
        if !status.success() {
            eprintln!("scenario '{name}': the cold-claim plan failed; refusing to report");
            return Ok(Outcome::Stopped(ExitCode::FAILURE));
        }
        // The shell assigned this from a pipeline under `set -e`. A plan
        // without a buffer count failed that assignment and ended the run.
        let Some(cold) = cold_buffers(&plan) else {
            return Ok(Outcome::Stopped(ExitCode::FAILURE));
        };

        let drain_file = self.work_file("drain.sql");
        fs::write_string(&drain_file, drain_sql(&query_text))?;
        let (status, drain) = support::capture(&mut self.psql_at(&["-f", &drain_file]))?;
        if !status.success() {
            eprintln!(
                "scenario '{name}': the drain failed part way; refusing to report a partial total"
            );
            return Ok(Outcome::Stopped(ExitCode::FAILURE));
        }
        println!("{}", result_line(name, &drain_total(&drain), &cold));
        if let Some(code) = Self::quiet(&mut self.psql_admin(&drop_db), false)? {
            return Ok(Outcome::Stopped(code));
        }
        Ok(Outcome::Reported)
    }
}

/// Arguments of `cargo xtask perf external-outbox-scan-matrix`.
#[derive(Debug, Args)]
pub struct Args {}

/// Runs `cargo xtask perf external-outbox-scan-matrix`.
pub fn run(_args: Args) -> Result<ExitCode> {
    run_in(&Ctx::from_env())
}

/// One scenario: its label, query file, index file and extra fixture file.
///
/// The files are names inside the scratch directory.
type Scenario = (
    &'static str,
    &'static str,
    Option<&'static str>,
    Option<&'static str>,
);

/// The four-way table that the page leads with.
const LEADING: [Scenario; 5] = [
    ("baseline: no index, legacy query", "legacy.sql", None, None),
    (
        "outer index only, legacy query",
        "legacy.sql",
        Some("idx_pending.sql"),
        None,
    ),
    ("rewrite only, no indexes", "rewritten.sql", None, None),
    (
        "all indexes, legacy query",
        "legacy.sql",
        Some("idx_all.sql"),
        None,
    ),
    (
        "all indexes + rewrite (as shipped)",
        "rewritten.sql",
        Some("idx_all.sql"),
        None,
    ),
];

/// The same drain under a stale row estimate.
const STALE: [Scenario; 4] = [
    ("stale: baseline", "legacy.sql", None, Some("stale.sql")),
    (
        "stale: all indexes, legacy query",
        "legacy.sql",
        Some("idx_all.sql"),
        Some("stale.sql"),
    ),
    (
        "stale: as shipped, executions join plain",
        "rewritten_plain_join.sql",
        Some("idx_all.sql"),
        Some("stale.sql"),
    ),
    (
        "stale: as shipped",
        "rewritten.sql",
        Some("idx_all.sql"),
        Some("stale.sql"),
    ),
];

fn run_in(ctx: &Ctx) -> Result<ExitCode> {
    let pgurl = support::var_or("PGURL", "postgres://postgres:postgres@localhost:5432");
    let work = fs::temp_dir()?;
    let matrix = Matrix {
        pgurl,
        root: ctx.root.clone(),
        work: work.path().to_path_buf(),
        db: format!("outbox_matrix_{}", std::process::id()),
    };

    fs::write_string(
        matrix.work.join("schema.sql"),
        schema_sql(&ctx.path("autumn-harvest/migrations"))?,
    )?;
    for (name, text) in [
        ("fixture.sql", FIXTURE_SQL),
        ("legacy.sql", LEGACY_SQL),
        ("rewritten.sql", REWRITTEN_SQL),
        ("rewritten_plain_join.sql", REWRITTEN_PLAIN_JOIN_SQL),
        ("idx_pending.sql", IDX_PENDING_SQL),
        ("idx_all.sql", IDX_ALL_SQL),
    ] {
        fs::write_string(matrix.work.join(name), text)?;
    }

    println!("== full 50-request drain, total buffers (pg_stat_statements, this database only) ==");
    println!("== repeat this script a few times: the sequential-scan scenarios vary  ==");
    println!("== by up to 3x run to run, and the shipped form barely moves           ==");
    if let Some(code) = run_all(&matrix, &LEADING)? {
        return Ok(code);
    }

    fs::write_string(matrix.work.join("stale.sql"), STALE_SQL)?;
    println!();
    println!("== the same drain under a row estimate left 400x above the truth ==");
    if let Some(code) = run_all(&matrix, &STALE)? {
        return Ok(code);
    }

    // Opt in with `OUTBOX_MATRIX_HISTORY=1`. Three more fixtures triple the
    // runtime, and the two tables above are the ones most readers want.
    let sweep: &[u32] = if support::non_empty_var("OUTBOX_MATRIX_HISTORY").is_some() {
        &[2000, 8000, 20000]
    } else {
        &[]
    };
    for &h in sweep {
        fs::write_string(matrix.work.join("history.sql"), history_sql(h))?;
        println!();
        println!("== {h} resolved requests already in the pending index ==");
        let pre = format!("history {h}: pre-change (no indexes)");
        let shipped = format!("history {h}: as shipped");
        for (name, query, indexes) in [
            (pre.as_str(), "legacy.sql", None),
            (shipped.as_str(), "rewritten.sql", Some("idx_all.sql")),
        ] {
            if let Outcome::Stopped(code) =
                matrix.run_scenario(name, query, indexes, Some("history.sql"))?
            {
                return Ok(code);
            }
        }
    }
    drop(work);
    Ok(ExitCode::SUCCESS)
}

/// Runs each scenario in order, and returns the exit code of the first that stops.
fn run_all(matrix: &Matrix, scenarios: &[Scenario]) -> Result<Option<ExitCode>> {
    for &(name, query, indexes, extra) in scenarios {
        if let Outcome::Stopped(code) = matrix.run_scenario(name, query, indexes, extra)? {
            return Ok(Some(code));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drain_projects_the_first_line_and_ends_with_a_bare_claim() {
        let drain = drain_sql(LEGACY_SQL);
        let lines: Vec<&str> = drain.lines().collect();
        assert!(lines[0].starts_with("SELECT pg_stat_statements_reset(0,"));
        assert_eq!(lines[1], "BEGIN;");
        assert_eq!(
            lines[2],
            "SELECT e.workflow_exec_id AS cexec, e.event_data->'data'->>'signal_id' AS csig FROM harvest_events e"
        );
        assert_eq!(drain.matches("\\gset\n").count(), 50);
        assert!(drain.contains("900000 + 50,"));
        assert!(!drain.contains("900000 + 51,"));
        assert!(drain.contains("FOR UPDATE OF e SKIP LOCKED\n;\nCOMMIT;\nSELECT sum("));
        assert!(drain.ends_with("datname = current_database());\n"));
    }

    #[test]
    fn drain_leaves_a_query_without_the_select_star_prefix_alone() {
        let drain = drain_sql("SELECT x.* FROM t\n");
        assert!(drain.contains("BEGIN;\nSELECT x.* FROM t\n\\gset\n"));
    }

    #[test]
    fn cold_buffers_takes_the_first_match_or_none() {
        let plan = "Limit\n  Buffers: shared hit=12 read=3\n  Buffers: shared hit=99\n";
        assert_eq!(cold_buffers(plan).as_deref(), Some("shared hit=12 read=3"));
        assert_eq!(
            cold_buffers("Buffers: shared hit=7, dirtied=1").as_deref(),
            Some("shared hit=7")
        );
        assert_eq!(cold_buffers("no buffers here"), None);
    }

    #[test]
    fn drain_total_reads_the_first_field_of_the_last_line() {
        assert_eq!(drain_total("BEGIN\nCOMMIT\n12345|52\n"), "12345");
        assert_eq!(drain_total("only"), "only");
        assert_eq!(drain_total(""), "");
    }

    #[test]
    fn result_line_pads_like_printf() {
        assert_eq!(
            result_line("a", "12", "shared hit=1"),
            format!(
                "{}{}",
                "a".to_string() + &" ".repeat(43),
                " drain=12           cold claim: shared hit=1"
            )
        );
    }

    #[test]
    fn history_sql_divides_by_one_thousand() {
        let sql = history_sql(8000);
        assert_eq!(sql.matches("generate_series(1, 8) gs;").count(), 2);
    }

    #[test]
    fn schema_sql_orders_directories_and_tolerates_a_missing_up_sql() {
        let dir = fs::temp_dir().unwrap();
        for name in ["2_b", "1_a"] {
            fs::create_dir_all(dir.path().join(name)).unwrap();
        }
        fs::write_string(dir.path().join("1_a/up.sql"), "CREATE TABLE a();\n").unwrap();
        let schema = schema_sql(dir.path()).unwrap();
        let root = dir.path().display();
        assert_eq!(
            schema,
            format!("-- ==== {root}/1_a/\nCREATE TABLE a();\n\n-- ==== {root}/2_b/\n\n")
        );
    }
}
