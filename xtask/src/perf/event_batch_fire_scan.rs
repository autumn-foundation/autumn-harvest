//! `cargo xtask perf event-batch-fire-scan`: the Ledger harness of the
//! due-row read in `event_batch::fire_due_on_conn`.
//! `docs/performance-event-batch-fire-scan.md` documents it.
//!
//! The capture test
//! `event_batch_fire_scan_perf::zz_capture_event_batch_fire_scan_evidence`
//! seeds open event batches at three sizes. At each size it fires one
//! unsharded scanner tick and then admits 200 starts, twice: once without
//! the `fire_at` index, once with it. So it reproduces the baseline on every
//! checkout.
//!
//! Usage:
//!
//! ```text
//! HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo xtask perf event-batch-fire-scan
//! ```
//!
//! `HARVEST_TEST_DATABASE_URL` is an admin URL. The capture copies one
//! migrated template database for each arm. The server needs
//! `pg_stat_statements` in `shared_preload_libraries`.
//!
//! It writes into `docs/perf-artifacts/event-batch-fire-scan/`.

use std::process::ExitCode;

use smop::prelude::*;

use super::support::{self, Capture, Ctx, Verdict};

/// The capture test that this command runs once.
const CAPTURE: Capture = Capture {
    filter: "event_batch_fire_scan_perf::zz_capture_event_batch_fire_scan_evidence",
    log: "/tmp/event_batch_fire_scan_capture.log",
    out_dir: "docs/perf-artifacts/event-batch-fire-scan",
    before_after: true,
    marker: "^equivalence confirmed:",
    verdict: Verdict::Equivalence,
};

/// Arguments of `cargo xtask perf event-batch-fire-scan`.
#[derive(Debug, Args)]
pub struct Args {}

/// Runs `cargo xtask perf event-batch-fire-scan`.
pub fn run(_args: Args) -> Result<ExitCode> {
    support::run_capture(&Ctx::from_env(), &CAPTURE)
}
