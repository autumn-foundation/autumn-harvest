//! `cargo xtask perf queue-pause-claim`: the port of
//! `autumn-harvest/scripts/queue_pause_claim_perf_repro.sh`.
//!
//! The reproduction harness for the queue-pause anti-join rewrite in
//! `queue::claim_task_query()` (issue #619, Ledger perf pass).
//! `docs/performance.md` documents it.
//!
//! The command runs the `#[ignore]`d evidence-capture test
//! `claim_budget_tests::zz_capture_queue_pause_claim_evidence` TWICE. Both runs
//! use the SAME harness and fixture generator in one invocation. Where the
//! harness falls back to a testcontainer, they also use the same freshly
//! created database.
//!
//! 1. "after" runs against the tree exactly as checked out. That tree has
//!    the fix, because this command ships alongside it.
//! 2. "before" runs against `queue.rs` with the fix temporarily removed. The
//!    command detects one of two ways to get there.
//!    - Dev loop: an uncommitted diff is present, and the fix is not yet
//!      committed. The command runs `git stash` on the file.
//!    - Clean checkout: there is no uncommitted diff, which is the normal
//!      state once the branch has merged. The command walks `git log` for
//!      `queue.rs` to the newest commit whose content does NOT contain the
//!      fix marker (`paused_queues AS MATERIALIZED`). It writes the content
//!      of that commit into the working file for the "before" capture.
//!
//!    Either way, the original working-tree file comes back on exit. That
//!    holds on success, on failure and on interrupt.
//!
//! The capture test detects which shape it measured. It inspects the text of
//! `queue::claim_task_query()` at runtime. So no hand-maintained "old" copy
//! of the query can drift out of sync. Both runs execute the real, compiled
//! function.
//!
//! Usage, with or without `HARVEST_TEST_DATABASE_URL`:
//!
//! ```text
//! HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo xtask perf queue-pause-claim
//!
//! # or, with only a reachable Docker daemon and no external Postgres:
//! cargo xtask perf queue-pause-claim
//! ```
//!
//! `HARVEST_TEST_DATABASE_URL`, when set, is an ADMIN URL.
//! `claim_bench_support.rs` treats it the same way elsewhere in this crate.
//! The harness creates, migrates, seeds, measures and drops a fresh, uniquely
//! named database per test run. When the variable is unset, `setup_bench_db()`
//! falls back to a testcontainer. This command does not gate on the variable,
//! so that fallback stays reachable. With neither an external database nor a
//! Docker daemon, the capture test skips loudly and writes no artifacts. See
//! `bench_db_or_skip()` in `claim_budget_tests.rs`.
//!
//! It writes into `docs/perf-artifacts/queue-pause-claim-anti-join/`:
//!
//! - `{before,after}-claim-backlog-{1000,10000,100000}.explain.txt`:
//!   `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)` for
//!   `claim_task_query()` at each published `BACKLOG_SWEEP` depth, with one of
//!   the four polled queues paused.
//! - `{before,after}-pg_stat_statements.txt`: a `pg_stat_statements` snapshot.
//!   It follows a drain of a 10k-row headline backlog by the REAL
//!   `claim_task()` Rust function, not by literal-substituted SQL.
//! - `{before,after}-fixture-summary.txt`: the seeded and claimable row counts
//!   and the paused queue name, per depth.
//!
//! Only `queue.rs` is toggled. The test files that carry the assertions of
//! this command stay exactly as checked out on both runs. The "before"
//! capture needs only the production query text to revert, not the tests
//! that assert its shape.
//!
//! Preconditions: a Rust toolchain that can build this crate. The command
//! also needs Docker for the testcontainer fallback of the harness, or a reachable
//! Postgres that `HARVEST_TEST_DATABASE_URL` names.

use std::process::ExitCode;

use smop::prelude::*;

use super::support::{self, Ctx, Toggle};

/// The capture test and fix marker of this command.
///
/// The artifact directory must match the fixed path that the Rust test
/// writes to. The test derives it from `CARGO_MANIFEST_DIR` at compile time,
/// so it cannot be overridden from here. It is display-only, for the final `ls`.
const TOGGLE: Toggle = Toggle {
    filter: "claim_budget_tests::zz_capture_queue_pause_claim_evidence",
    out_dir: "docs/perf-artifacts/queue-pause-claim-anti-join",
    fix_marker: "paused_queues AS MATERIALIZED",
    log_stem: "/tmp/queue_pause_claim_capture_",
};

/// Arguments of `cargo xtask perf queue-pause-claim`.
#[derive(Debug, Args)]
pub struct Args {}

/// Runs `cargo xtask perf queue-pause-claim`.
pub fn run(_args: Args) -> Result<ExitCode> {
    support::run_toggle(&Ctx::from_env(), &TOGGLE)
}
