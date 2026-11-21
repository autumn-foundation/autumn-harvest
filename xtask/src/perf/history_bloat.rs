//! `cargo xtask perf history-bloat`: the port of
//! `autumn-harvest-plugin/scripts/history_bloat_perf_repro.sh`.
//!
//! The reproduction harness for the buffer-cost measurement of
//! `min_history_events` and `history_bloat_min_events`.
//! `docs/performance-history-bloat-filter.md` documents it.
//!
//! The command seeds a production-shaped `harvest_events` fixture directly
//! against a real, migrated Postgres database. The fixture holds 5,000
//! ordinary executions with 5 to 80 events each. It also holds 15
//! long-running "bloated" executions with 50k to 450k events each. That is
//! the exact shape that the `history_bloat_min_events` early-warning filter
//! (issue #704) exists to find.
//!
//! The command then captures `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)`
//! for the pre-fix and post-fix forms of the correlated-subquery threshold
//! predicate. `load_workflows`, `load_stalled_workflows` and
//! `load_history_bloat_workflows` use that predicate
//! (`autumn-harvest-plugin/src/api.rs`).
//!
//! Usage:
//!
//! ```text
//! HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo xtask perf history-bloat [out-dir]
//! ```
//!
//! `HARVEST_TEST_DATABASE_URL` is an ADMIN URL, as in `claim_bench_support.rs`.
//! The command creates, migrates, seeds, measures and then drops a fresh,
//! uniquely named database. The role that the URL names must be able to
//! `CREATE DATABASE` and `DROP DATABASE`.
//!
//! It takes roughly 2 to 3 minutes. The seed of about 2.9M event rows
//! dominates the runtime.

use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, ExitStatus, Stdio};

use smop::prelude::*;

use super::support::{self, Ctx};

/// The `count(*) >= threshold` bound that both forms test.
const THRESHOLD: u32 = 10000;

/// The fixture: the two cohorts of executions and their events.
const SEED_SQL: &str = r#"-- 5,000 ordinary RUNNING executions, 5-80 recorded events each.
INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, shard_id, state, input, queue_name)
SELECT gen_random_uuid(), 'order_flow', 'healthy-' || i, 0, 'RUNNING', '{}'::jsonb, 'default'
FROM generate_series(1, 5000) AS s(i);

-- 15 long-running RUNNING executions, 50k-450k recorded events each --
-- the shape `history_bloat_min_events` (issue #704) exists to surface.
INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, shard_id, state, input, queue_name)
SELECT gen_random_uuid(), 'poll_loop', 'bloated-' || i, 0, 'RUNNING', '{}'::jsonb, 'default'
FROM generate_series(1, 15) AS s(i);

-- The per-row event count is derived deterministically from `md5(workflow_id)`
-- so re-runs reproduce identical counts. `('x' || hex)::bit(32)::int` gives a
-- SIGNED 32-bit interpretation of the hash bits -- roughly half of all hashes
-- have the high bit set and cast to a NEGATIVE int, which would make `% N`
-- (Postgres preserves the sign of the dividend) negative too, collapsing the
-- lower bound below zero and silently seeding a zero-event row via an empty
-- `generate_series` instead of the intended range. Casting to `bigint` before
-- `abs()` avoids the one remaining edge case (`abs(int4 MIN)` overflows int4;
-- it fits comfortably in bigint), so the modulus operand is always
-- non-negative and the intended [lower, lower+N) range is honored for every
-- hash. See PR #1173 review discussion for the measured impact of the bug
-- this replaced (~46% of rows in both cohorts previously got zero events).
INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
SELECT
    e.id,
    ev.i,
    CASE WHEN ev.i % 2 = 0 THEN 'ActivityScheduled' ELSE 'ActivityCompleted' END,
    CASE WHEN ev.i % 2 = 0 THEN
        jsonb_build_object('type','ActivityScheduled','data',jsonb_build_object(
            'activity_id', gen_random_uuid(), 'name','charge_card','input',
            jsonb_build_object('order_id', e.workflow_id, 'amount_cents', 1999, 'items', jsonb_build_array('sku-1','sku-2'))))
    ELSE
        jsonb_build_object('type','ActivityCompleted','data',jsonb_build_object(
            'activity_id', gen_random_uuid(), 'output',
            jsonb_build_object('charge_id','ch_' || ev.i, 'status','ok')))
    END,
    NOW() - ((80 - ev.i) || ' seconds')::interval
FROM harvest_workflow_executions e
CROSS JOIN LATERAL generate_series(1, 5 + (abs((('x' || substr(md5(e.workflow_id), 1, 8))::bit(32)::int)::bigint) % 75)) AS ev(i)
WHERE e.workflow_name = 'order_flow';

INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
SELECT
    e.id,
    ev.i,
    CASE WHEN ev.i % 3 = 0 THEN 'TimerFired' ELSE 'MarkerRecorded' END,
    CASE WHEN ev.i % 3 = 0 THEN
        jsonb_build_object('type','TimerFired','data',jsonb_build_object('timer_id','poll-' || ev.i))
    ELSE
        jsonb_build_object('type','MarkerRecorded','data',jsonb_build_object(
            'name','poll_iteration', 'details', jsonb_build_object('iteration', ev.i, 'batch', jsonb_build_array(1,2,3,4,5))))
    END,
    NOW() - (((50000 + (abs((('x' || substr(md5(e.workflow_id), 1, 8))::bit(32)::int)::bigint) % 400000)) - ev.i) || ' seconds')::interval
FROM harvest_workflow_executions e
CROSS JOIN LATERAL generate_series(1, 50000 + (abs((('x' || substr(md5(e.workflow_id), 1, 8))::bit(32)::int)::bigint) % 400000)) AS ev(i)
WHERE e.workflow_name = 'poll_loop';

-- VACUUM (not just ANALYZE) sets the visibility map. An un-vacuumed table
-- can make an Index Only Scan's "Heap Fetches" count differ from run to run
-- purely based on autovacuum timing, independent of query shape -- this bit
-- the real-endpoint measurement in PR #1173 review (see
-- docs/performance-history-bloat-filter.md "Verification against the real
-- production entry point" and real_query_probe.rs). VACUUM here so every
-- capture below reads from matched, deterministic visibility-map state.
VACUUM ANALYZE harvest_events;
VACUUM ANALYZE harvest_workflow_executions;
"#;

/// Replaces the database of a connection URL, as `urlsplit` and `urlunsplit` did.
///
/// The path becomes `/<db>`. The scheme is lowercased, and the query and the
/// fragment stay. An empty query, fragment or netloc is dropped, as
/// `urlunsplit` drops it.
pub fn with_database(url: &str, db: &str) -> String {
    let (scheme, rest) = match url.split_once(':') {
        Some((scheme, rest))
            if scheme
                .chars()
                .next()
                .is_some_and(|first| first.is_ascii_alphabetic())
                && scheme
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.')) =>
        {
            (Some(scheme.to_ascii_lowercase()), rest)
        }
        _ => (None, url),
    };
    let (rest, fragment) = rest.split_once('#').map_or((rest, ""), |(r, f)| (r, f));
    let (rest, query) = rest.split_once('?').map_or((rest, ""), |(r, q)| (r, q));
    let netloc = rest.strip_prefix("//").map(|after| {
        let end = after.find('/').unwrap_or(after.len());
        &after[..end]
    });
    let mut out = String::new();
    if let Some(scheme) = scheme {
        out.push_str(&scheme);
        out.push(':');
    }
    // `urlunsplit` writes `//` only for a non-empty netloc of this scheme.
    if let Some(netloc) = netloc.filter(|netloc| !netloc.is_empty()) {
        out.push_str("//");
        out.push_str(netloc);
    }
    out.push('/');
    out.push_str(db);
    if !query.is_empty() {
        out.push('?');
        out.push_str(query);
    }
    if !fragment.is_empty() {
        out.push('#');
        out.push_str(fragment);
    }
    out
}

/// The SQL of the seven EXPLAIN captures, by artifact label.
pub fn captures(bloated: &str, healthy: &str) -> Vec<(&'static str, String)> {
    let offset = THRESHOLD - 1;
    let t = THRESHOLD;
    vec![
        (
            "before-count-star-bloated",
            format!(
                "SELECT (SELECT COUNT(*) FROM harvest_events WHERE workflow_exec_id = '{bloated}') >= {t} AS result;"
            ),
        ),
        (
            "before-count-star-healthy",
            format!(
                "SELECT (SELECT COUNT(*) FROM harvest_events WHERE workflow_exec_id = '{healthy}') >= {t} AS result;"
            ),
        ),
        (
            "after-bounded-exists-bloated",
            format!(
                "SELECT EXISTS (SELECT 1 FROM harvest_events WHERE workflow_exec_id = '{bloated}' ORDER BY event_id OFFSET {offset} LIMIT 1) AS result;"
            ),
        ),
        (
            "after-bounded-exists-healthy",
            format!(
                "SELECT EXISTS (SELECT 1 FROM harvest_events WHERE workflow_exec_id = '{healthy}' ORDER BY event_id OFFSET {offset} LIMIT 1) AS result;"
            ),
        ),
        // The same bounded EXISTS/OFFSET/LIMIT form WITHOUT the ORDER BY. It
        // backs the doc comment of `apply_min_history_events_filter`, which
        // says the ORDER BY is load-bearing, not decorative. Without a sort
        // target, the planner has no reason to walk the composite index in
        // order. For a high-selectivity `workflow_exec_id` predicate, it can
        // still fall back to a Seq Scan, even with the bound present.
        (
            "after-bounded-exists-no-order-by-bloated",
            format!(
                "SELECT EXISTS (SELECT 1 FROM harvest_events WHERE workflow_exec_id = '{bloated}' OFFSET {offset} LIMIT 1) AS result;"
            ),
        ),
        // An isolated correlated-SubPlan comparison against the FULL outer
        // table, with no other endpoint filters or columns. It backs the
        // per-scan buffer-count claim of the section
        // "verification-against-the-real-production-entry-point" of
        // `docs/performance-history-bloat-filter.md`. The `count(*)` wrapper
        // forces the predicate over every row, so the scan cannot stop at the
        // first match.
        //
        // These are the SAME two queries that produced the committed
        // `correlated-before-count-star.explain.txt` and
        // `correlated-after-bounded-exists.explain.txt` artifacts. A rerun
        // after a fixture or query change regenerates ALL committed evidence
        // from one code path. So a stale, manually captured artifact can
        // never sit beside freshly regenerated ones (PR #1173,
        // discussion_r3787605008).
        (
            "correlated-before-count-star",
            format!(
                "SELECT count(*) FROM (SELECT id FROM harvest_workflow_executions WHERE (SELECT COUNT(*) FROM harvest_events WHERE workflow_exec_id = harvest_workflow_executions.id) >= {t}) sub;"
            ),
        ),
        (
            "correlated-after-bounded-exists",
            format!(
                "SELECT count(*) FROM (SELECT id FROM harvest_workflow_executions WHERE EXISTS (SELECT 1 FROM harvest_events WHERE workflow_exec_id = harvest_workflow_executions.id ORDER BY event_id OFFSET {offset} LIMIT 1)) sub;"
            ),
        ),
    ]
}

/// The admin connection and the scratch database of one run.
struct Bench {
    admin: String,
    db_name: String,
    db_url: String,
}

impl Bench {
    fn psql(url: &str, args: &[&str]) -> Command {
        let mut command = Command::new("psql");
        command.arg(url).args(args);
        command
    }

    /// Returns `psql "$DB_URL" <args>`.
    fn db(&self, args: &[&str]) -> Command {
        Self::psql(&self.db_url, args)
    }

    /// Runs `psql "$DB_URL" -tAc <sql>` and returns its output, as `$(...)` did.
    fn scalar(&self, sql: &str) -> Result<std::result::Result<String, ExitCode>> {
        let (status, out) = support::capture(&mut self.db(&["-tAc", sql]))?;
        Ok(if status.success() {
            Ok(out)
        } else {
            Err(support::exit_code(status))
        })
    }
}

/// Drops the scratch database on exit, as the `EXIT` trap did.
struct DropDatabase<'a>(&'a Bench);

impl Drop for DropDatabase<'_> {
    fn drop(&mut self) {
        println!("== dropping {} ==", self.0.db_name);
        let sql = format!("DROP DATABASE IF EXISTS {} WITH (FORCE);", self.0.db_name);
        let mut command = Bench::psql(&self.0.admin, &["-v", "ON_ERROR_STOP=1", "-c", &sql]);
        command.stderr(Stdio::null());
        support::cleanup(&mut command);
    }
}

/// Arguments of `cargo xtask perf history-bloat`.
#[derive(Debug, Args)]
pub struct Args {
    /// The artifact directory. The default is `docs/perf-artifacts/history-bloat-filter`.
    pub out_dir: Option<PathBuf>,
}

/// Runs `cargo xtask perf history-bloat`.
pub fn run(args: Args) -> Result<ExitCode> {
    let Some(admin) = support::non_empty_var(support::DB_URL_VAR) else {
        eprintln!(
            "HARVEST_TEST_DATABASE_URL: set HARVEST_TEST_DATABASE_URL to an admin connection \
             string, e.g. postgres://postgres:postgres@localhost:5432/postgres"
        );
        return Ok(ExitCode::FAILURE);
    };
    let ctx = Ctx::from_env();
    let out_dir = args
        .out_dir
        .filter(|dir| !dir.as_os_str().is_empty())
        .unwrap_or_else(|| ctx.path("docs/perf-artifacts/history-bloat-filter"));
    run_in(&ctx, admin, &out_dir)
}

/// Exits with the code of a failed step.
macro_rules! step {
    ($status:expr) => {{
        let status = $status;
        if !status.success() {
            return Ok(support::exit_code(status));
        }
    }};
}

/// Unwraps a `$(psql ...)` value, or exits with the code of the failed psql.
macro_rules! value {
    ($result:expr) => {
        match $result? {
            Ok(value) => value,
            Err(code) => return Ok(code),
        }
    };
}

/// Captures the `pg_stat_statements` snapshot of both forms.
///
/// It returns the status of the last step that ran, as `step!` reads it.
fn snapshot(bench: &Bench, out_dir: &Path, bloated_id: &str) -> Result<ExitStatus> {
    println!("== pg_stat_statements snapshot (requires the extension) ==");
    for sql in [
        "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;",
        "SELECT pg_stat_statements_reset();",
    ] {
        let mut command = bench.db(&["-c", sql]);
        command.stderr(Stdio::null());
        let _ = support::status_to(&mut command, Stdio::null())?;
    }
    let offset = THRESHOLD - 1;
    for sql in [
        format!(
            "SELECT (SELECT COUNT(*) FROM harvest_events WHERE workflow_exec_id = '{bloated_id}') >= {THRESHOLD};"
        ),
        format!(
            "SELECT EXISTS (SELECT 1 FROM harvest_events WHERE workflow_exec_id = '{bloated_id}' ORDER BY event_id OFFSET {offset} LIMIT 1);"
        ),
    ] {
        let status = support::status_to(&mut bench.db(&["-c", &sql]), Stdio::null())?;
        if !status.success() {
            return Ok(status);
        }
    }
    let snapshot = out_dir.join("pg_stat_statements.txt");
    let file = support::open_out(&snapshot, false)?;
    let err = file.try_clone().context("cloning the snapshot handle")?;
    let mut command = bench.db(&[
        "-c",
        "SELECT query, calls, shared_blks_hit + shared_blks_read AS total_buffers, shared_blks_read, temp_blks_written FROM pg_stat_statements WHERE query ILIKE '%harvest_events%' ORDER BY total_buffers DESC;",
    ]);
    command.stderr(err);
    let status = support::status_to(&mut command, file)?;
    if !status.success() {
        let line = "pg_stat_statements unavailable (extension not preloaded) — skipped\n";
        print!("{line}");
        fs::write_string(&snapshot, line)?;
    }
    Ok(ExitStatus::default())
}

/// Concatenates each `up.sql` in directory order, each followed by a newline.
///
/// This is `for d in $(ls -d "$MIGRATIONS_DIR"/*/ | sort); do cat "$d/up.sql"; echo; done`.
/// A directory without an `up.sql` fails, as the failed `cat` did.
pub fn migration_bundle(migrations: &Path) -> Result<String> {
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(migrations)
        .with_context(|| format!("listing {}", migrations.display()))?
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_dir()
                && !path
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with('.'))
        })
        .collect();
    dirs.sort();
    let mut bundle = String::new();
    for dir in dirs {
        bundle.push_str(&fs::read_string(dir.join("up.sql"))?);
        bundle.push('\n');
    }
    Ok(bundle)
}

fn run_in(ctx: &Ctx, admin: String, out_dir: &Path) -> Result<ExitCode> {
    let db_name = format!(
        "harvest_history_bloat_perf_{}_{}",
        std::process::id(),
        time::now().timestamp()
    );
    let migrations = ctx.path("autumn-harvest/migrations");

    println!("== creating {db_name} ==");
    step!(support::status_to(
        &mut Bench::psql(
            &admin,
            &[
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                &format!("CREATE DATABASE {db_name};")
            ]
        ),
        Stdio::null(),
    )?);

    let db_url = with_database(&admin, &db_name);
    let bench = Bench {
        admin,
        db_name,
        db_url,
    };
    let _cleanup = DropDatabase(&bench);

    println!("== applying migrations ==");
    let bundle = migration_bundle(&migrations)?;
    step!(support::status_with_stdin(
        bench
            .db(&["-v", "ON_ERROR_STOP=1", "-q", "-f", "-"])
            .stdout(Stdio::null()),
        &bundle,
    )?);

    println!("== seeding fixture (this is the slow part) ==");
    step!(support::status_with_stdin(
        &mut bench.db(&["-v", "ON_ERROR_STOP=1", "-q"]),
        SEED_SQL
    )?);

    // One representative execution from each cohort. The bloated one has
    // the largest count, the worst case that COUNT(*) has to materialize in
    // full. The healthy one is arbitrary.
    let bloated_id = value!(bench.scalar(
        "SELECT id FROM harvest_workflow_executions e JOIN LATERAL (SELECT COUNT(*) c FROM harvest_events WHERE workflow_exec_id = e.id) c ON true ORDER BY c.c DESC LIMIT 1;"
    ));
    let healthy_id = value!(
        bench.scalar("SELECT id FROM harvest_workflow_executions WHERE workflow_id = 'healthy-1';")
    );
    let bloated_count = value!(bench.scalar(&format!(
        "SELECT COUNT(*) FROM harvest_events WHERE workflow_exec_id = '{bloated_id}';"
    )));
    let healthy_count = value!(bench.scalar(&format!(
        "SELECT COUNT(*) FROM harvest_events WHERE workflow_exec_id = '{healthy_id}';"
    )));
    let total_events = value!(bench.scalar("SELECT COUNT(*) FROM harvest_events;"));
    let total_executions =
        value!(bench.scalar("SELECT COUNT(*) FROM harvest_workflow_executions;"));

    println!("== bloated execution: {bloated_id} ({bloated_count} events) ==");
    println!("== healthy execution: {healthy_id} ({healthy_count} events) ==");
    println!("== fixture totals: {total_events} events across {total_executions} executions ==");

    fs::create_dir_all(out_dir)?;
    fs::write_string(
        out_dir.join("fixture-summary.txt"),
        format!(
            "bloated_execution_id={bloated_id}\nbloated_execution_events={bloated_count}\n\
             healthy_execution_id={healthy_id}\nhealthy_execution_events={healthy_count}\n\
             fixture_total_events={total_events}\nfixture_total_executions={total_executions}\n"
        ),
    )?;

    for (label, sql) in captures(&bloated_id, &healthy_id) {
        let file = out_dir.join(format!("{label}.explain.txt"));
        fs::write_string(&file, format!("-- {label} --\n"))?;
        let explain = format!("EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF) {sql}");
        step!(support::status_to(
            &mut bench.db(&["-v", "ON_ERROR_STOP=1", "-c", &explain]),
            support::open_out(&file, true)?,
        )?);
        println!("captured {}", file.display());
    }

    step!(snapshot(&bench, out_dir, &bloated_id)?);

    println!("== done. Artifacts in {} ==", out_dir.display());
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_database_swaps_only_the_path() {
        assert_eq!(
            with_database("postgres://postgres:pw@localhost:5432/postgres", "fresh"),
            "postgres://postgres:pw@localhost:5432/fresh"
        );
        assert_eq!(
            with_database("postgresql://u@h/db?sslmode=disable#x", "fresh"),
            "postgresql://u@h/fresh?sslmode=disable#x"
        );
        assert_eq!(
            with_database("POSTGRES://h:1", "fresh"),
            "postgres://h:1/fresh"
        );
        assert_eq!(
            with_database("postgres://h/db?", "fresh"),
            "postgres://h/fresh"
        );
    }

    #[test]
    fn with_database_matches_urlunsplit_on_odd_inputs() {
        assert_eq!(with_database("host=localhost dbname=x", "fresh"), "/fresh");
        assert_eq!(with_database("postgres:///db", "fresh"), "postgres:/fresh");
        assert_eq!(
            with_database("postgres://u:p#w@h/db", "fresh"),
            "postgres://u:p/fresh#w@h/db"
        );
        assert_eq!(
            with_database("postgres://[::1]:5432/db?a=b", "fresh"),
            "postgres://[::1]:5432/fresh?a=b"
        );
    }

    #[test]
    fn captures_use_the_threshold_and_its_offset() {
        let all = captures("B", "H");
        assert_eq!(all.len(), 7);
        assert_eq!(
            all[0].1,
            "SELECT (SELECT COUNT(*) FROM harvest_events WHERE workflow_exec_id = 'B') >= 10000 AS result;"
        );
        assert!(
            all[3]
                .1
                .contains("'H' ORDER BY event_id OFFSET 9999 LIMIT 1")
        );
        assert!(!all[4].1.contains("ORDER BY"));
        assert_eq!(all[6].0, "correlated-after-bounded-exists");
    }
}
