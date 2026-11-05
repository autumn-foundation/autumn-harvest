//! `cargo xtask perf deep-backlog-ledger`: the port of
//! `autumn-harvest/scripts/deep_backlog_ledger_repro.sh`.
//!
//! The Ledger capture against the seeded deep-backlog fixture (issue #1956).
//! The findings are in `docs/performance-deep-backlog.md`.
//!
//! Usage:
//!
//! ```text
//! HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo xtask perf deep-backlog-ledger
//! ```
//!
//! With `HARVEST_TEST_DATABASE_URL` unset, the harness starts a `postgres:16`
//! container with `pg_stat_statements` preloaded. Docker must be reachable.
//!
//! The URL is an admin URL. The harness creates one database per run and
//! seeds it. It drives the claim workload, and it snapshots
//! `pg_stat_statements` and `pg_stat_user_tables`. It then drops the
//! database. An external server needs `pg_stat_statements` in
//! `shared_preload_libraries`.
//!
//! Knobs, all optional. The capture test reads them.
//!
//! - `HARVEST_DEEP_BACKLOG_SEED`: the fixture seed (default 1956).
//! - `HARVEST_DEEP_BACKLOG_ROWS`: the live task rows of the deep run (default
//!   1000000).
//! - `HARVEST_DEEP_BACKLOG_SECS`: the wall-clock bound of each workload
//!   (default 600).
//! - `HARVEST_DEEP_BACKLOG_OUT`: the artifact directory (default
//!   `docs/perf-artifacts/deep-backlog`).
//!
//! It writes these files for each of the `shallow` and `deep` runs:
//!
//! - `<run>-post-seed-pg_stat_user_tables.txt`: the table stats after the seed.
//! - `<run>-workload-pg_stat_user_tables.txt`: the table counter deltas of the
//!   workload.
//! - `<run>-pg_stat_statements.txt`: the statements of the workload.
//!
//! It also writes `fixture-summary.txt` for both runs.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use smop::prelude::*;

use super::support::{self, Ctx};

/// The capture test of this command.
const TEST_FILTER: &str = "deep_backlog_fixture_tests::zz_capture_deep_backlog_ledger_evidence";

/// The completion line that the capture test prints.
const MARKER: &str = "^== capture complete: artifacts in ";

/// Arguments of `cargo xtask perf deep-backlog-ledger`.
#[derive(Debug, Args)]
pub struct Args {}

/// Runs `cargo xtask perf deep-backlog-ledger`.
pub fn run(_args: Args) -> Result<ExitCode> {
    run_in(&Ctx::from_env())
}

/// Creates a fresh, empty log file, as `mktemp -t deep_backlog_ledger.XXXXXX.log` did.
fn make_log(dir: &Path) -> Result<PathBuf> {
    let seed = time::now().timestamp_subsec_nanos() ^ std::process::id();
    for attempt in 0..64_u32 {
        let suffix = format!(
            "{:06x}",
            (seed.wrapping_add(attempt.wrapping_mul(7919))) & 0x00ff_ffff
        );
        let path = dir.join(format!("deep_backlog_ledger.{suffix}.log"));
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(_) => return Ok(path),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => {
                return Err(error).with_context(|| format!("creating {}", path.display()));
            }
        }
    }
    bail!("could not create a log file in {}", dir.display())
}

/// The `cargo test` arguments of the capture.
const CAPTURE_ARGS: [&str; 10] = [
    "test",
    "-p",
    "autumn-harvest",
    "--test",
    "integration",
    "--",
    "--ignored",
    "--nocapture",
    "--exact",
    TEST_FILTER,
];

fn run_in(ctx: &Ctx) -> Result<ExitCode> {
    let log = make_log(&std::env::temp_dir())?;
    println!(
        "== capturing via {TEST_FILTER}; log in {} ==",
        log.display()
    );
    let status = support::tee_merged(&mut ctx.cargo(CAPTURE_ARGS), &log)?;
    if !status.success() {
        eprintln!("FATAL: the capture test failed. See {}.", log.display());
        return Ok(ExitCode::FAILURE);
    }
    if support::grep_count(&log, MARKER)? == 0 {
        eprintln!(
            "FATAL: the capture did not complete. The test failed, or it skipped for lack of \
             a database. See {}.",
            log.display()
        );
        return Ok(ExitCode::FAILURE);
    }
    println!("== done ==");
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn make_log_creates_distinct_files_with_the_script_name() {
        let dir = fs::temp_dir().unwrap();
        let first = make_log(dir.path()).unwrap();
        let second = make_log(dir.path()).unwrap();
        assert_ne!(first, second);
        let name = first.file_name().unwrap().to_string_lossy().into_owned();
        assert!(name.starts_with("deep_backlog_ledger."), "{name}");
        assert_eq!(first.extension().and_then(|ext| ext.to_str()), Some("log"));
        assert!(first.is_file());
    }

    #[test]
    fn capture_runs_the_exact_test_without_extra_features() {
        assert!(CAPTURE_ARGS.contains(&"--exact"));
        assert!(!CAPTURE_ARGS.contains(&"--features"));
        assert_eq!(CAPTURE_ARGS.last(), Some(&TEST_FILTER));
    }
}
