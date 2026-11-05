//! `cargo xtask perf notify-stage-probe`: the port of
//! `autumn-harvest/scripts/notify_stage_probe_perf_repro.sh`.
//!
//! The Ledger capture of the NOTIFY stage probes per workflow on the e2e
//! workload (issue #2125).
//!
//! Usage:
//!
//! ```text
//! HARVEST_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:5433/postgres \
//!   cargo xtask perf notify-stage-probe <label>
//! ```
//!
//! The server needs `pg_stat_statements` in `shared_preload_libraries`. The
//! command resets the statistics and runs the e2e throughput scenario. It
//! writes `docs/perf-artifacts/notify-stage-probe/<label>.txt` with the probe
//! calls per workflow and the calls ranking. The bench log goes to `target/`.
//! Wall-clock is not recorded.
//!
//! Knobs, all optional:
//!
//! - `HARVEST_BENCH_WORKFLOWS`: the measured completions (default 200).
//! - `NOTIFY_STAGE_PROBE_OUT`: the artifact directory.

use std::process::ExitCode;

use smop::prelude::*;

use super::pool_recycle_ping::{EchoPsql, bench_args, run_ranking};
use super::support::{self, Ctx};

/// The summary query: probe calls per workflow, and the share of all calls.
const SUMMARY_SQL: &str = r#"
    WITH s AS (SELECT * FROM pg_stat_statements
               WHERE query NOT ILIKE '%pg_stat_statements%'),
    w AS (SELECT sum(calls) AS n FROM s
          WHERE query LIKE 'INSERT INTO "harvest_workflow_executions"%'),
    p AS (SELECT coalesce(sum(calls), 0) AS n FROM s
          WHERE query LIKE 'SELECT txid_current%AS txid, current_database()%')
    SELECT (SELECT n FROM w) AS workflows,
           sum(calls) AS total_calls,
           (SELECT n FROM p) AS probe_calls,
           round((SELECT n FROM p)::numeric / nullif((SELECT n FROM w), 0), 3)
             AS probes_per_workflow,
           round(100.0 * (SELECT n FROM p) / sum(calls), 2) AS probe_pct_of_calls,
           sum(shared_blks_hit + shared_blks_read) AS total_blocks,
           sum(temp_blks_written) AS temp_blks_written,
           sum(wal_bytes) AS wal_bytes
    FROM s"#;

/// Arguments of `cargo xtask perf notify-stage-probe`.
#[derive(Debug, Args)]
pub struct Args {
    /// The label of this capture. It names the artifact and the bench log.
    pub label: Option<String>,
}

/// Runs `cargo xtask perf notify-stage-probe`.
pub fn run(args: Args) -> Result<ExitCode> {
    let Some(label) = args.label.filter(|label| !label.is_empty()) else {
        eprintln!("usage: cargo xtask perf notify-stage-probe <label>");
        return Ok(ExitCode::FAILURE);
    };
    let Some(url) = support::non_empty_var(support::DB_URL_VAR) else {
        eprintln!("HARVEST_TEST_DATABASE_URL: set HARVEST_TEST_DATABASE_URL (admin URL)");
        return Ok(ExitCode::FAILURE);
    };
    let ctx = Ctx::from_env();
    let out = support::non_empty_var("NOTIFY_STAGE_PROBE_OUT").map_or_else(
        || ctx.path("docs/perf-artifacts/notify-stage-probe"),
        Into::into,
    );
    let log = ctx.path(format!("target/notify-stage-probe-{label}-bench.log"));
    fs::create_dir_all(&out)?;
    fs::create_dir_all(ctx.path("target"))?;

    let psql = EchoPsql::new(&ctx, &url);
    let status = psql.reset()?;
    if !status.success() {
        return Ok(support::exit_code(status));
    }

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
    let code = run_ranking(&ctx, &url, &label, SUMMARY_SQL, None, &artifact)?;
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
    fn summary_counts_probes_against_inserted_workflows() {
        assert!(SUMMARY_SQL.starts_with("\n    WITH s AS"));
        assert!(SUMMARY_SQL.contains(r#"'INSERT INTO "harvest_workflow_executions"%'"#));
        assert!(SUMMARY_SQL.contains("'SELECT txid_current%AS txid, current_database()%'"));
        assert!(SUMMARY_SQL.ends_with("    FROM s"));
    }

    #[test]
    fn a_missing_label_is_a_usage_error() {
        let code = run(Args { label: None }).unwrap();
        assert_eq!(code, ExitCode::FAILURE);
    }
}
