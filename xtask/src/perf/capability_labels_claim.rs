//! `cargo xtask perf capability-labels-claim`: the port of
//! `autumn-harvest/scripts/capability_labels_claim_perf_repro.sh`.
//!
//! The reproduction harness for the capability-labels claim-predicate
//! measurement (issue #382, Ledger perf pass). `docs/performance-capability-labels.md`
//! documents it.
//!
//! Unlike `cargo xtask perf queue-pause-claim`, this command does NOT toggle a
//! code fix. `queue::claim_task_query()` is unmodified end to end. Measurement
//! found no query-shape fix that helps, and the doc page says why. The command
//! runs the `#[ignore]`d evidence-capture test
//! `claim_budget_tests::zz_capture_capability_labels_claim_evidence` ONCE.
//!
//! That single test drives the real `claim_task_query()` and `claim_task()` at
//! TWO seeded data states of `harvest_task_queue.required_capabilities`. One
//! state is `NULL`, the default today. In the other, every row carries one
//! `Exact` requirement that the `labels` of the claiming worker satisfy. The
//! test captures both, so this command has nothing else to toggle.
//!
//! Usage, with or without `HARVEST_TEST_DATABASE_URL`:
//!
//! ```text
//! HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo xtask perf capability-labels-claim
//!
//! # or, with only a reachable Docker daemon and no external Postgres:
//! cargo xtask perf capability-labels-claim
//! ```
//!
//! `HARVEST_TEST_DATABASE_URL`, when set, is an ADMIN URL. `claim_bench_support.rs`
//! treats it the same way elsewhere in this crate. The harness creates,
//! migrates, seeds, measures and drops a fresh, uniquely named database per
//! run. When the variable is unset, `claim_bench_support::db::setup_bench_db`
//! falls back to a testcontainer. This command does not gate on the variable,
//! so that fallback stays reachable. With neither an external database nor a
//! Docker daemon, the capture test skips loudly and writes no artifacts. See
//! `bench_db_or_skip()` in `claim_budget_tests.rs`.
//!
//! It writes into `docs/perf-artifacts/capability-labels-claim-predicate/`:
//!
//! - `{no-capabilities,capability-labels}-claim-backlog-{1000,10000,100000}.explain.txt`:
//!   `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)` for
//!   `claim_task_query()` at each published `BACKLOG_SWEEP` depth, at both
//!   seeded states.
//! - `{no-capabilities,capability-labels}-pg_stat_statements.txt`: a
//!   `pg_stat_statements` snapshot at both seeded states. The snapshot follows
//!   a drain of the 10k-row headline backlog by the REAL `claim_task()` Rust
//!   function, not by literal-substituted SQL.
//! - `fixture-summary.txt`: the seeded and claimable row counts per depth, and
//!   the claimed-row count of each stat-snapshot drain. As a sanity check, the
//!   test asserts that the two labels claim equal counts.
//!
//! Preconditions: a Rust toolchain that can build this crate. The command also needs Docker
//! for the testcontainer fallback of the harness, or a reachable Postgres that
//! `HARVEST_TEST_DATABASE_URL` names.

use std::process::ExitCode;

use smop::prelude::*;

use super::support::{self, Capture, Ctx, Verdict};

/// The capture test that this command runs once.
const CAPTURE: Capture = Capture {
    filter: "claim_budget_tests::zz_capture_capability_labels_claim_evidence",
    log: "/tmp/capability_labels_claim_capture.log",
    out_dir: "docs/perf-artifacts/capability-labels-claim-predicate",
    before_after: false,
    marker: "^== capture complete: capability-labels evidence, artifacts in ",
    verdict: Verdict::Completion,
};

/// Arguments of `cargo xtask perf capability-labels-claim`.
#[derive(Debug, Args)]
pub struct Args {}

/// Runs `cargo xtask perf capability-labels-claim`.
pub fn run(_args: Args) -> Result<ExitCode> {
    support::run_capture(&Ctx::from_env(), &CAPTURE)
}
