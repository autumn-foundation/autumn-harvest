//! `cargo xtask perf build-reachability-fanout`: the port of
//! `autumn-harvest/scripts/build_reachability_fanout_perf_repro.sh`.
//!
//! The reproduction harness for the build-reachability fan-out fix (Ledger perf
//! pass), documented in `docs/performance-build-reachability-fanout.md`.
//!
//! The evidence-capture test
//! `build_reachability_fanout_perf::zz_capture_build_reachability_fanout_evidence`
//! seeds a production-shaped fixture into a throwaway database. It then
//! captures `pg_stat_statements` and result-set evidence for two strategies in
//! one run. "before" is a loop over the unchanged per-build
//! `build_reachability()` helper. The pre-fix `all_build_reachability()` did
//! exactly that internally. "after" is the real, shipped
//! `all_build_reachability()`.
//!
//! Usage, with or without `HARVEST_TEST_DATABASE_URL`:
//!
//! ```text
//! HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo xtask perf build-reachability-fanout
//!
//! # or, with only a reachable Docker daemon and no external Postgres:
//! cargo xtask perf build-reachability-fanout
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
//! It writes into `docs/perf-artifacts/build-reachability-fanout/`:
//!
//! - `{before,after}.pg_stat_statements.txt`: a `pg_stat_statements` snapshot
//!   for each strategy. It is filtered to build-id-bearing statements and
//!   ranked by total buffers.
//! - `{before,after}.result-rows.txt`: the per-build reachability counters
//!   that each strategy computed. They are byte-identical between the two.
//!   That is the equivalence proof of this fix.
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
    filter: "build_reachability_fanout_perf::zz_capture_build_reachability_fanout_evidence",
    log: "/tmp/build_reachability_fanout_capture.log",
    out_dir: "docs/perf-artifacts/build-reachability-fanout",
    before_after: true,
    marker: "^equivalence confirmed:",
    verdict: Verdict::Equivalence,
};

/// Arguments of `cargo xtask perf build-reachability-fanout`.
#[derive(Debug, Args)]
pub struct Args {}

/// Runs `cargo xtask perf build-reachability-fanout`.
pub fn run(_args: Args) -> Result<ExitCode> {
    support::run_capture(&Ctx::from_env(), &CAPTURE)
}
