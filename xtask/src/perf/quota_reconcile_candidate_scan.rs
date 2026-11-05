//! `cargo xtask perf quota-reconcile-candidate-scan`: the port of `autumn-harvest/scripts/quota_reconcile_candidate_scan_perf_repro.sh`.
//!
//! The reproduction harness for the residual `workflow_name` filter of
//! `quota_reconcile::CANDIDATE_SQL` (issue #1226).
//! `docs/performance-quota-reconcile-candidate-scan.md` documents it.
//!
//! Usage, with or without `HARVEST_TEST_DATABASE_URL`:
//!
//! ```text
//! HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo xtask perf quota-reconcile-candidate-scan
//!
//! # or, with only a reachable Docker daemon and no external Postgres:
//! cargo xtask perf quota-reconcile-candidate-scan
//! ```
//!
//! `HARVEST_TEST_DATABASE_URL`, when set, is an ADMIN URL.
//! `claim_bench_support.rs` treats it the same way elsewhere in this crate.
//! The harness creates, migrates, seeds and measures against a fresh, uniquely
//! named database per run.
//!
//! It does NOT drop that database when the run ends, as `setup_bench_db`
//! documents. It holds an idle lease connection open instead. So the database
//! stays visible to `pg_stat_activity`, and it defends itself against the
//! stale-database sweep of a concurrent run. The `setup_bench_db` call of a
//! FUTURE run sweeps and drops databases that a prior run left stale. The
//! teardown of this run does not.
//!
//! A one-off local run against a real Postgres server therefore leaves a
//! `harvest_claim_bench_*` database of about 600,000 rows behind. It stays
//! until the next run of this or any other suite against the same server.
//! `DROP DATABASE` it by hand if that matters. The testcontainer fallback does
//! not have this problem. When the variable is unset,
//! `claim_bench_support::db::setup_bench_db` falls back to a testcontainer,
//! and the daemon reclaims it entirely on its own.
//!
//! It writes into `docs/perf-artifacts/quota-reconcile-candidate-scan/`:
//!
//! - `noise-{20000,100000,500000}.explain.txt`:
//!   `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)` for the unmodified
//!   candidate scan, first tick, at each noise-population size.
//! - `alternative-index-explain.txt`: the SAME unmodified query at the largest
//!   size, with the rejected `(workflow_name, id)` partial index of the
//!   migration comment also present.
//! - `pg_stat_statements.txt`: a `pg_stat_statements` snapshot after a real,
//!   complete `reconcile_quota_keys_from()` pass at the largest size.
//! - `fixture-summary.txt`: the row counts per size and the tick and backfill
//!   totals of the full pass.
//!
//! Preconditions: a Rust toolchain that can build this crate. The command also needs Docker
//! for the testcontainer fallback of the harness, or a reachable Postgres that
//! `HARVEST_TEST_DATABASE_URL` names. That server needs `pg_stat_statements` in `shared_preload_libraries`.

use std::process::ExitCode;

use smop::prelude::*;

use super::broken_session_scan::support::{self, Capture, Ctx, Verdict};

/// The capture test that this command runs once.
const CAPTURE: Capture = Capture {
    filter: "quota_reconcile_candidate_scan_perf_tests::zz_capture_quota_reconcile_candidate_scan_evidence",
    log: "/tmp/quota_reconcile_candidate_scan_capture.log",
    out_dir: "docs/perf-artifacts/quota-reconcile-candidate-scan",
    before_after: false,
    marker: "^== capture complete: artifacts in ",
    verdict: Verdict::Completion,
};

/// Arguments of `cargo xtask perf quota-reconcile-candidate-scan`.
#[derive(Debug, Args)]
pub struct Args {}

/// Runs `cargo xtask perf quota-reconcile-candidate-scan`.
pub fn run(_args: Args) -> Result<ExitCode> {
    support::run_capture(&Ctx::from_env(), &CAPTURE)
}
