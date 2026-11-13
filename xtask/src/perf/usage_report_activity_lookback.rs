//! `cargo xtask perf usage-report-activity-lookback`: the port of
//! `autumn-harvest/scripts/usage_report_activity_lookback_perf_repro.sh`.
//!
//! The reproduction harness for the usage-report activity-lookback index
//! (issue #596, Ledger perf pass).
//! `docs/performance-usage-report-activity-lookback.md` documents it.
//!
//! The evidence-capture test
//! `usage_report_activity_lookback_tests::zz_capture_usage_report_activity_lookback_evidence`
//! runs one invocation against ONE seeded fixture. It drops the candidate index
//! unconditionally before the "before" capture. So it reproduces the pre-fix
//! baseline whether or not
//! `20260905181020_harvest_usage_activity_lookback_index` has already run
//! against the target database. It then creates the index and captures the
//! "after" form. The query text is the same both times. Only the schema
//! changes.
//!
//! Usage, with or without `HARVEST_TEST_DATABASE_URL`:
//!
//! ```text
//! HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo xtask perf usage-report-activity-lookback
//!
//! # or, with only a reachable Docker daemon and no external Postgres:
//! cargo xtask perf usage-report-activity-lookback
//! ```
//!
//! `HARVEST_TEST_DATABASE_URL`, when set, is an ADMIN URL. `claim_bench_support.rs`
//! treats it the same way elsewhere in this crate. The harness creates,
//! migrates, seeds, measures and drops a fresh, uniquely named database per
//! run. When the variable is unset, `claim_bench_support::db::setup_bench_db`
//! falls back to a testcontainer. This command does not gate on the variable,
//! so that fallback stays reachable. With neither an external database nor a
//! Docker daemon, the capture test skips loudly and writes no artifacts.
//!
//! It writes into `docs/perf-artifacts/usage-report-activity-lookback/`:
//!
//! - `{before,after}.explain.txt`:
//!   `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)` for
//!   `usage::usage_sql()`. The fixture is production-shaped: 40,000 executions
//!   and about 450,000 events. It has a skewed 1% "batch" tail of 50 to 300
//!   activities per execution.
//! - `{before,after}.pg_stat_statements.txt`: a `pg_stat_statements` snapshot
//!   after the execution of each form.
//! - `{before,after}.result-rows.txt`: the sorted grouped-usage result set of
//!   each form. The two are byte-identical. That is the equivalence proof of
//!   this fix.
//!
//! Preconditions: a Rust toolchain that can build this crate. The command also needs Docker
//! for the testcontainer fallback of the harness, or a reachable Postgres that
//! `HARVEST_TEST_DATABASE_URL` names. That server needs `pg_stat_statements` in
//! `shared_preload_libraries`. The harness creates the extension itself. The
//! module must already be preloaded at postmaster start, because
//! `CREATE EXTENSION` alone cannot enable it after the fact.

use std::process::ExitCode;

use smop::prelude::*;

use super::support::{self, Capture, Ctx, Verdict};

/// The capture test that this command runs once.
const CAPTURE: Capture = Capture {
    filter: "usage_report_activity_lookback_tests::zz_capture_usage_report_activity_lookback_evidence",
    log: "/tmp/usage_report_activity_lookback_capture.log",
    out_dir: "docs/perf-artifacts/usage-report-activity-lookback",
    before_after: true,
    marker: "^equivalence confirmed:",
    verdict: Verdict::Equivalence,
};

/// Arguments of `cargo xtask perf usage-report-activity-lookback`.
#[derive(Debug, Args)]
pub struct Args {}

/// Runs `cargo xtask perf usage-report-activity-lookback`.
pub fn run(_args: Args) -> Result<ExitCode> {
    support::run_capture(&Ctx::from_env(), &CAPTURE)
}
