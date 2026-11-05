//! `cargo xtask perf broken-session-scan`: the reproduction harness for the
//! broken-session scan index fix (Ledger perf pass, issue #2069).
//!
//! `docs/performance-broken-session-scan.md` documents the fix. This command
//! is the port of `autumn-harvest/scripts/broken_session_scan_perf_repro.sh`.
//!
//! The capture test seeds a throwaway database at three sizes. It runs
//! `sessions::enforce_broken_sessions` once per size. It writes
//! `pg_stat_statements`, EXPLAIN, index statistics and a state dump to
//! `docs/perf-artifacts/broken-session-scan/<label>-*`.
//!
//! "before" runs with the new migration moved aside. "after" runs with it in
//! place. The command restores the migration on exit.
//!
//! Usage:
//!
//! ```text
//! HARVEST_TEST_DATABASE_URL=postgres://postgres@localhost:5432/postgres \
//!   cargo xtask perf broken-session-scan
//! ```
//!
//! The role must be a superuser, and the server must preload
//! `pg_stat_statements`. Without `HARVEST_TEST_DATABASE_URL` the test starts a
//! Postgres 16 container.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use smop::prelude::*;

#[path = "support.rs"]
pub mod support;

use support::Ctx;

/// The migration directory suffix that the "before" run moves aside.
const MIGRATION_SUFFIX: &str = "_harvest_task_queue_session_active_index";

/// The artifact directory, relative to the repository root.
const OUT_DIR: &str = "docs/perf-artifacts/broken-session-scan";

/// Arguments of `cargo xtask perf broken-session-scan`.
#[derive(Debug, Args)]
pub struct Args {}

/// Runs `cargo xtask perf broken-session-scan`.
pub fn run(_args: Args) -> Result<ExitCode> {
    run_in(&Ctx::from_env())
}

/// Moves the migration back when the command exits, as the `EXIT` trap did.
struct Aside {
    migration: PathBuf,
    aside: fs::TempDir,
}

impl Aside {
    fn parked(&self) -> PathBuf {
        self.aside
            .path()
            .join(self.migration.file_name().unwrap_or_default())
    }

    fn move_back(&self) -> Result<ExitCode> {
        mv(&self.parked(), &self.migration)
    }
}

impl Drop for Aside {
    fn drop(&mut self) {
        if self.parked().is_dir() {
            let _ = self.move_back();
        }
    }
}

/// Runs `mv from to` and returns its exit code.
fn mv(from: &Path, to: &Path) -> Result<ExitCode> {
    // `mv` and not `rename`: the scratch directory can be on another file system.
    let status = sh::cmd("mv").arg(from).arg(to).status()?;
    Ok(support::exit_code(status))
}

/// Finds the one migration directory that the fix added.
fn find_migration(root: &Path) -> Result<PathBuf> {
    let pattern = format!(
        "{}/autumn-harvest/migrations/*{MIGRATION_SUFFIX}",
        root.display()
    );
    let mut found: Vec<PathBuf> = fs::glob(&pattern)?
        .into_iter()
        .filter(|path| path.is_dir())
        .collect();
    found.sort();
    match found.len() {
        1 => Ok(found.remove(0)),
        0 => bail!("no migration matches {pattern}"),
        _ => bail!("more than one migration matches {pattern}: {found:?}"),
    }
}

/// The `cargo test` arguments of one capture. `PERF_LABEL` names the half.
const CAPTURE_ARGS: [&str; 10] = [
    "test",
    "-p",
    "autumn-harvest",
    "--all-features",
    "--test",
    "integration",
    "--",
    "--ignored",
    "zz_capture_broken_session_scan_perf_evidence",
    "--nocapture",
];

fn run_capture(ctx: &Ctx, label: &str) -> Result<ExitCode> {
    let mut command = ctx.cargo(CAPTURE_ARGS);
    command.env("PERF_LABEL", label);
    let status = support::status(&mut command)?;
    Ok(support::exit_code(status))
}

fn run_in(ctx: &Ctx) -> Result<ExitCode> {
    let migration = find_migration(&ctx.root)?;
    let guard = Aside {
        migration,
        aside: fs::temp_dir()?,
    };

    let code = mv(&guard.migration, guard.aside.path())?;
    if code != ExitCode::SUCCESS {
        return Ok(code);
    }
    let code = run_capture(ctx, "before")?;
    if code != ExitCode::SUCCESS {
        return Ok(code);
    }
    let code = guard.move_back()?;
    if code != ExitCode::SUCCESS {
        return Ok(code);
    }
    let code = run_capture(ctx, "after")?;
    if code != ExitCode::SUCCESS {
        return Ok(code);
    }

    let out = ctx.path(OUT_DIR);
    for n in [100, 400, 1600] {
        let status = sh::cmd("cmp")
            .arg(out.join(format!("before-state-n{n}.txt")))
            .arg(out.join(format!("after-state-n{n}.txt")))
            .status()?;
        if !status.success() {
            return Ok(support::exit_code(status));
        }
        println!("n={n}: state identical");
    }
    let status = sh::cmd("cat")
        .arg(out.join("before-sweep.txt"))
        .arg(out.join("after-sweep.txt"))
        .status()?;
    Ok(support::exit_code(status))
}
