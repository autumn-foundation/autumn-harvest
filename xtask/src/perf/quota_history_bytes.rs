//! `cargo xtask perf quota-history-bytes`: the port of `autumn-harvest/scripts/quota_history_bytes_perf_repro.sh`.
//!
//! The reproduction harness for the quota `history_bytes` admission-check
//! measurement (issue #946 AC7, Ledger perf pass).
//! `docs/performance-quota-history-bytes.md` documents it.
//!
//! Like `cargo xtask perf capability-labels-claim`, this command does NOT
//! toggle a code fix. `quota::quota_usage_query()` is unmodified end to end.
//! Measurement found no query-shape rewrite that helps. A `LATERAL` rewrite was
//! tried and measured worse, as the doc page records. The command runs the
//! `#[ignore]`d evidence-capture test
//! `quota_history_bytes_perf_tests::zz_capture_quota_history_bytes_evidence`
//! ONCE.
//!
//! That single test seeds the fixed footprint of one target tenant: 1,000
//! executions and 178,000 events. It seeds that footprint against THREE
//! background-table sizes, `NOISE_SWEEP = [3, 15, 100]`. Those sizes are about
//! 205k, 313k and 1.08M total `harvest_events` rows. The test captures all
//! three. It also captures one negative-result `LATERAL` variant at the
//! smallest size. So this command has nothing else to toggle.
//!
//! Usage, with or without `HARVEST_TEST_DATABASE_URL`:
//!
//! ```text
//! HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo xtask perf quota-history-bytes
//!
//! # or, with only a reachable Docker daemon and no external Postgres:
//! cargo xtask perf quota-history-bytes
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
//! It writes into `docs/perf-artifacts/quota-history-bytes-admission/`:
//!
//! - `noise_mult-{3,15,100}.explain.txt`:
//!   `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)` for the real
//!   `quota::quota_usage_query()` text at each background-table size. The own
//!   1,000-execution footprint of the target tenant stays fixed.
//! - `lateral-variant-negative-result.explain.txt`: the plan of the rejected
//!   `LATERAL` rewrite at the smallest size.
//! - `pg_stat_statements.txt`: a `pg_stat_statements` snapshot at the largest
//!   fixture. It follows 20 calls of the REAL `quota::load_quota_usage()` Rust
//!   function, not of literal-substituted SQL.
//! - `fixture-summary.txt`: the exact
//!   `(active_executions, history_bytes, dead_letters)` of the fixture at each
//!   sweep point. As a sanity check, the test asserts that these are identical
//!   across all three sizes.
//!
//! Preconditions: a Rust toolchain that can build this crate. The command also needs Docker
//! for the testcontainer fallback of the harness, or a reachable Postgres that
//! `HARVEST_TEST_DATABASE_URL` names.

use std::process::ExitCode;

use smop::prelude::*;

use super::broken_session_scan::support::{self, Capture, Ctx, Verdict};

/// The capture test that this command runs once.
const CAPTURE: Capture = Capture {
    filter: "quota_history_bytes_perf_tests::zz_capture_quota_history_bytes_evidence",
    log: "/tmp/quota_history_bytes_capture.log",
    out_dir: "docs/perf-artifacts/quota-history-bytes-admission",
    before_after: false,
    marker: "^== capture complete: artifacts in ",
    verdict: Verdict::Completion,
};

/// Arguments of `cargo xtask perf quota-history-bytes`.
#[derive(Debug, Args)]
pub struct Args {}

/// Runs `cargo xtask perf quota-history-bytes`.
pub fn run(_args: Args) -> Result<ExitCode> {
    support::run_capture(&Ctx::from_env(), &CAPTURE)
}
