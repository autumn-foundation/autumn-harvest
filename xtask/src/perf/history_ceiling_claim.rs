//! `cargo xtask perf history-ceiling-claim`: the port of `autumn-harvest/scripts/history_ceiling_claim_perf_repro.sh`.
//!
//! The reproduction harness for the workflow-history-ceiling scanner query fix
//! in `timeout::workflow_history_ceiling_query()` (issue #493, Ledger perf
//! pass). `docs/performance-history-ceiling.md` documents it.
//!
//! Unlike `cargo xtask perf concurrency-key-claim` and
//! `cargo xtask perf queue-pause-claim`, this command does NOT toggle git
//! state. The evidence-capture test
//! `history_ceiling_claim_tests::zz_capture_history_ceiling_claim_evidence`
//! carries the exact pre-fix query text as a hardcoded `BEFORE_SQL` constant.
//! It runs the "before" and the "after" form against the SAME seeded fixture
//! in ONE test invocation. So there is nothing to stash or check out. The
//! "before" half also cannot drift from what actually shipped.
//!
//! Usage, with or without `HARVEST_TEST_DATABASE_URL`:
//!
//! ```text
//! HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo xtask perf history-ceiling-claim
//!
//! # or, with only a reachable Docker daemon and no external Postgres:
//! cargo xtask perf history-ceiling-claim
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
//! It writes into `docs/perf-artifacts/history-ceiling-scanner/`:
//!
//! - `{before,after}-history-ceiling.explain.txt`:
//!   `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)` for the
//!   pre-fix and post-fix text of `workflow_history_ceiling_query()`. The
//!   fixture is production-shaped: 100,000 executions, 3,000 RUNNING and about
//!   4,000,000 events, with ceiling=5,000.
//! - `{before,after}-pg_stat_statements.txt`: a `pg_stat_statements` snapshot
//!   after the execution of each form. See "Known limitation" in the doc page.
//!   This snapshot has come back empty in every observed run.
//! - `{before,after}-result-rows.txt`: the sorted `(id, event_count)` result
//!   set of each form. The two are byte-identical. That is the equivalence
//!   proof of this fix.
//!
//! Preconditions: a Rust toolchain that can build this crate. The command also needs Docker
//! for the testcontainer fallback of the harness, or a reachable Postgres that
//! `HARVEST_TEST_DATABASE_URL` names.

use std::process::ExitCode;

use smop::prelude::*;

use super::support::{self, Capture, Ctx, Verdict};

/// The capture test that this command runs once.
const CAPTURE: Capture = Capture {
    filter: "history_ceiling_claim_tests::zz_capture_history_ceiling_claim_evidence",
    log: "/tmp/history_ceiling_claim_capture.log",
    out_dir: "docs/perf-artifacts/history-ceiling-scanner",
    before_after: true,
    marker: "^equivalence confirmed:",
    verdict: Verdict::Equivalence,
};

/// Arguments of `cargo xtask perf history-ceiling-claim`.
#[derive(Debug, Args)]
pub struct Args {}

/// Runs `cargo xtask perf history-ceiling-claim`.
pub fn run(_args: Args) -> Result<ExitCode> {
    support::run_capture(&Ctx::from_env(), &CAPTURE)
}
