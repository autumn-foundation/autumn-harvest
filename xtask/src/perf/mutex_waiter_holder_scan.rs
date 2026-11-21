//! `cargo xtask perf mutex-waiter-holder-scan`: the Ledger harness of the
//! terminal-sweep key read in `mutex::sweep_terminal_holder_and_wake`.
//! `docs/performance-mutex-waiter-holder-scan.md` documents it.
//!
//! The capture test
//! `mutex_waiter_holder_scan_perf::zz_capture_mutex_waiter_holder_scan_evidence`
//! seeds a mutex waiter fixture at three sizes. At each size it drives 200
//! workflow runs to their end twice: once without the waiter index, once
//! with it. So it reproduces the baseline on every checkout.
//!
//! Usage, with or without `HARVEST_TEST_DATABASE_URL`:
//!
//! ```text
//! HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo xtask perf mutex-waiter-holder-scan
//! ```
//!
//! `HARVEST_TEST_DATABASE_URL` is an admin URL. Without it, the harness
//! falls back to a testcontainer. The server needs `pg_stat_statements` in
//! `shared_preload_libraries`.
//!
//! It writes into `docs/perf-artifacts/mutex-waiter-holder-scan/`.

use std::process::ExitCode;

use smop::prelude::*;

use super::support::{self, Capture, Ctx, Verdict};

/// The capture test that this command runs once.
const CAPTURE: Capture = Capture {
    filter: "mutex_waiter_holder_scan_perf::zz_capture_mutex_waiter_holder_scan_evidence",
    log: "/tmp/mutex_waiter_holder_scan_capture.log",
    out_dir: "docs/perf-artifacts/mutex-waiter-holder-scan",
    before_after: true,
    marker: "^equivalence confirmed:",
    verdict: Verdict::Equivalence,
};

/// Arguments of `cargo xtask perf mutex-waiter-holder-scan`.
#[derive(Debug, Args)]
pub struct Args {}

/// Runs `cargo xtask perf mutex-waiter-holder-scan`.
pub fn run(_args: Args) -> Result<ExitCode> {
    support::run_capture(&Ctx::from_env(), &CAPTURE)
}
