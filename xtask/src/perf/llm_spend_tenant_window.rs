//! `cargo xtask perf llm-spend-tenant-window`: the Ledger harness of the
//! tenant sum in `llm_budget::load_llm_spend`.
//! `docs/performance-llm-spend-tenant-window.md` documents it.
//!
//! The capture test
//! `llm_spend_tenant_window_perf::zz_capture_llm_spend_tenant_window_evidence`
//! seeds 200,000 runs and about 6.3 million ledger rows into a fresh
//! database. It drops the candidate index before the "before" capture, so it
//! reproduces the baseline on every checkout. It then builds the index and
//! captures the "after" form. The statement text is the same both times.
//!
//! Usage, with or without `HARVEST_TEST_DATABASE_URL`:
//!
//! ```text
//! HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo xtask perf llm-spend-tenant-window
//! ```
//!
//! `HARVEST_TEST_DATABASE_URL` is an admin URL. Without it, the harness
//! falls back to a testcontainer. The server needs `pg_stat_statements` in
//! `shared_preload_libraries`.
//!
//! It writes into `docs/perf-artifacts/llm-spend-tenant-window/`.

use std::process::ExitCode;

use smop::prelude::*;

use super::support::{self, Capture, Ctx, Verdict};

/// The capture test that this command runs once.
const CAPTURE: Capture = Capture {
    filter: "llm_spend_tenant_window_perf::zz_capture_llm_spend_tenant_window_evidence",
    log: "/tmp/llm_spend_tenant_window_capture.log",
    out_dir: "docs/perf-artifacts/llm-spend-tenant-window",
    before_after: true,
    marker: "^equivalence confirmed:",
    verdict: Verdict::Equivalence,
};

/// Arguments of `cargo xtask perf llm-spend-tenant-window`.
#[derive(Debug, Args)]
pub struct Args {}

/// Runs `cargo xtask perf llm-spend-tenant-window`.
pub fn run(_args: Args) -> Result<ExitCode> {
    support::run_capture(&Ctx::from_env(), &CAPTURE)
}
