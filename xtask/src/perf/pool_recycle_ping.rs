//! `cargo xtask perf pool-recycle-ping`: the port of
//! `autumn-harvest/scripts/pool_recycle_ping_perf_repro.sh`.
//!
//! The Ledger capture of the pool recycle pings per claimed activity on the
//! e2e workload.
//!
//! Usage:
//!
//! ```text
//! HARVEST_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:5433/postgres \
//!   cargo xtask perf pool-recycle-ping <label>
//! ```
//!
//! The server needs `pg_stat_statements` in `shared_preload_libraries`. The
//! command resets the statistics and runs the e2e throughput scenario. It
//! writes `docs/perf-artifacts/pool-recycle-ping/<label>.txt` with the calls
//! ranking and the pings per claimed activity. Wall-clock is not recorded.
//!
//! Knobs, all optional:
//!
//! - `HARVEST_BENCH_WORKFLOWS`: the measured completions (default 200).
//! - `POOL_RECYCLE_OUT`: the artifact directory.
//!
//! `cargo xtask perf notify-stage-probe` shares the psql and bench helpers of
//! this module.

use std::io;
use std::path::Path;
use std::process::{Command, ExitCode, ExitStatus};

use smop::prelude::*;

use super::support::{self, Ctx, Tee};

/// The summary query: pings per claimed activity, and the share of all calls.
const SUMMARY_SQL: &str = r"
    WITH s AS (SELECT * FROM pg_stat_statements
               WHERE query NOT ILIKE '%pg_stat_statements%'),
    t AS (SELECT sum(calls) AS c FROM s),
    a AS (SELECT sum(calls) AS n FROM s
          WHERE query LIKE 'UPDATE harvest_task_queue SET state = $3, worker_id = $4, started_at = $5, scheduled_at%')
    SELECT (SELECT n FROM a) AS claimed_activities,
           (SELECT c FROM t) AS total_calls,
           coalesce(sum(calls) FILTER (WHERE query = 'SELECT $1'), 0) AS ping_calls,
           round(coalesce(sum(calls) FILTER (WHERE query = 'SELECT $1'), 0)::numeric
                 / nullif((SELECT n FROM a), 0), 3) AS pings_per_activity,
           round(100.0 * coalesce(sum(calls) FILTER (WHERE query = 'SELECT $1'), 0)
                 / (SELECT c FROM t), 2) AS ping_pct_of_calls,
           sum(shared_blks_hit + shared_blks_read) AS total_blocks,
           sum(temp_blks_written) AS temp_blks_written,
           sum(wal_bytes) AS wal_bytes
    FROM s";

/// The ten statements with the most calls. Both e2e captures print it.
pub const TOP_TEN_SQL: &str = r"
    SELECT calls, shared_blks_hit + shared_blks_read AS blks,
           left(regexp_replace(query, '\s+', ' ', 'g'), 100) AS query
    FROM pg_stat_statements WHERE query NOT ILIKE '%pg_stat_statements%'
    ORDER BY calls DESC LIMIT 10";

/// The plan of the ping statement itself.
const PING_EXPLAIN_SQL: &str = "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS) SELECT 1";

/// Builds the `psql "$URL" -X` commands of the e2e captures.
pub struct EchoPsql<'a> {
    ctx: &'a Ctx,
    url: &'a str,
}

impl<'a> EchoPsql<'a> {
    /// Returns a builder for `url`.
    pub const fn new(ctx: &'a Ctx, url: &'a str) -> Self {
        Self { ctx, url }
    }

    /// Returns `psql "$URL" -X <args>`.
    pub fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new("psql");
        command
            .arg(self.url)
            .arg("-X")
            .args(args)
            .current_dir(&self.ctx.root);
        command
    }

    /// Runs `psql "$URL" -X -qAt -c "SELECT pg_stat_statements_reset()" >/dev/null`.
    pub fn reset(&self) -> Result<ExitStatus> {
        support::status_to(
            &mut self.command(&["-qAt", "-c", "SELECT pg_stat_statements_reset()"]),
            std::process::Stdio::null(),
        )
    }
}

/// The `cargo bench` arguments of the e2e throughput scenario.
pub const fn bench_args() -> [&'static str; 7] {
    [
        "bench",
        "-p",
        "autumn-harvest",
        "--features",
        "db,testing",
        "--bench",
        "e2e_bench",
    ]
}

/// Prints the ranking group and writes it to `artifact`, as `{ ...; } | tee` did.
///
/// psql keeps its stderr. A failed psql stops the group with its exit code.
pub fn run_ranking(
    ctx: &Ctx,
    url: &str,
    label: &str,
    summary_sql: &str,
    explain: Option<(&str, &str)>,
    artifact: &Path,
) -> Result<ExitCode> {
    let mut file = support::open_out(artifact, false)?;
    let mut sink = Tee {
        first: io::stdout(),
        second: &mut file,
    };
    let psql = EchoPsql::new(ctx, url);
    let mut steps: Vec<(String, &str)> = vec![
        (format!("== label: {label}\n"), summary_sql),
        ("== top 10 by calls\n".to_string(), TOP_TEN_SQL),
    ];
    if let Some((header, sql)) = explain {
        steps.push((format!("{header}\n"), sql));
    }
    for (header, sql) in steps {
        io::Write::write_all(&mut sink, header.as_bytes()).context("writing the artifact")?;
        let status = support::pipe_stdout(&mut psql.command(&["-c", sql]), &mut sink)?;
        if !status.success() {
            return Ok(support::exit_code(status));
        }
    }
    Ok(ExitCode::SUCCESS)
}

/// Arguments of `cargo xtask perf pool-recycle-ping`.
#[derive(Debug, Args)]
pub struct Args {
    /// The label of this capture. It names the artifact and the bench log.
    pub label: Option<String>,
}

/// Runs `cargo xtask perf pool-recycle-ping`.
pub fn run(args: Args) -> Result<ExitCode> {
    let Some(label) = args.label.filter(|label| !label.is_empty()) else {
        eprintln!("usage: cargo xtask perf pool-recycle-ping <label>");
        return Ok(ExitCode::FAILURE);
    };
    let Some(url) = support::non_empty_var(support::DB_URL_VAR) else {
        eprintln!("HARVEST_TEST_DATABASE_URL: set HARVEST_TEST_DATABASE_URL (admin URL)");
        return Ok(ExitCode::FAILURE);
    };
    let ctx = Ctx::from_env();
    let out = support::non_empty_var("POOL_RECYCLE_OUT").map_or_else(
        || ctx.path("docs/perf-artifacts/pool-recycle-ping"),
        Into::into,
    );
    fs::create_dir_all(&out)?;

    let status = EchoPsql::new(&ctx, &url).reset()?;
    if !status.success() {
        return Ok(support::exit_code(status));
    }

    let log = out.join(format!("{label}-bench.log"));
    let mut bench = ctx.cargo(bench_args());
    bench
        .env("HARVEST_BENCH_SCENARIOS", "throughput")
        .env("HARVEST_BENCH_SHARDS", "1")
        .env(
            "HARVEST_BENCH_WORKFLOWS",
            support::var_or("HARVEST_BENCH_WORKFLOWS", "200"),
        );
    let log_file = support::open_out(&log, false)?;
    let err_file = log_file
        .try_clone()
        .context("cloning the bench log handle")?;
    bench.stderr(err_file);
    if !support::status_to(&mut bench, log_file)?.success() {
        eprintln!("FATAL: bench failed. See {}", log.display());
        return Ok(ExitCode::FAILURE);
    }

    let artifact = out.join(format!("{label}.txt"));
    let explain = Some(("== EXPLAIN of the ping statement", PING_EXPLAIN_SQL));
    let code = run_ranking(&ctx, &url, &label, SUMMARY_SQL, explain, &artifact)?;
    if code != ExitCode::SUCCESS {
        return Ok(code);
    }
    println!("== done: {}", artifact.display());
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_queries_keep_the_placeholders_that_the_shell_escaped() {
        assert!(SUMMARY_SQL.contains("SET state = $3, worker_id = $4, started_at = $5"));
        assert!(SUMMARY_SQL.contains("WHERE query = 'SELECT $1'"));
        assert!(!SUMMARY_SQL.contains('\\'));
        assert!(TOP_TEN_SQL.contains(r"regexp_replace(query, '\s+', ' ', 'g')"));
    }

    #[test]
    fn the_bench_runs_the_e2e_target_with_db_and_testing() {
        assert_eq!(
            bench_args().join(" "),
            "bench -p autumn-harvest --features db,testing --bench e2e_bench"
        );
    }

    #[test]
    fn a_missing_label_is_a_usage_error() {
        let code = run(Args {
            label: Some(String::new()),
        })
        .unwrap();
        assert_eq!(code, ExitCode::FAILURE);
    }
}
