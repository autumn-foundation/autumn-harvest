//! `cargo xtask perf schedule-to-close-claim`: the port of `autumn-harvest/scripts/schedule_to_close_claim_perf_repro.sh`.
//!
//! The reproduction harness for the `schedule_to_close_at` claim-predicate
//! measurement (issue #378, Ledger perf pass).
//! `docs/performance-schedule-to-close.md` documents it.
//!
//! Like `cargo xtask perf capability-labels-claim`, this command does NOT
//! toggle a code fix. `queue::claim_task_query()` is unmodified end to end.
//! This predicate is a plain inline column test
//! (`schedule_to_close_at IS NULL OR schedule_to_close_at > NOW()`), not a
//! subquery. So there is no query-shape fix to try. The command runs the
//! `#[ignore]`d evidence-capture test
//! `claim_budget_tests::zz_capture_schedule_to_close_claim_evidence` ONCE.
//!
//! That single test drives the real `claim_task_query()` and `claim_task()` at
//! TWO seeded data states of `harvest_task_queue.schedule_to_close_at`. One
//! state is `NULL`, the default today. In the other, every row carries a
//! deadline 100 years in the future. That deadline never elapses, however long
//! the capture runs, so it excludes nothing. The test captures both states, so
//! this command has nothing else to toggle.
//!
//! Usage, with or without `HARVEST_TEST_DATABASE_URL`:
//!
//! ```text
//! HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo xtask perf schedule-to-close-claim
//!
//! # or, with only a reachable Docker daemon and no external Postgres:
//! cargo xtask perf schedule-to-close-claim
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
//! It writes into `docs/perf-artifacts/schedule-to-close-claim-predicate/`:
//!
//! - `{no-schedule-to-close,schedule-to-close}-claim-backlog-{1000,10000,100000}.explain.txt`:
//!   `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)` for
//!   `claim_task_query()` at each published `BACKLOG_SWEEP` depth, at both
//!   seeded states.
//! - `{no-schedule-to-close,schedule-to-close}-pg_stat_statements.txt`: a
//!   `pg_stat_statements` snapshot at both seeded states. It follows a drain of
//!   the 10k-row headline backlog by the REAL `claim_task()` Rust function, not
//!   by literal-substituted SQL.
//! - `fixture-summary.txt`: the seeded and claimable row counts per depth, and
//!   the claimed-row count of each stat-snapshot drain. As a sanity check, the
//!   test asserts that the two labels claim equal counts.
//!
//! Preconditions: a Rust toolchain that can build this crate. The command also needs Docker
//! for the testcontainer fallback of the harness, or a reachable Postgres that
//! `HARVEST_TEST_DATABASE_URL` names.
//!
//! An external Postgres instance has two more requirements. It needs
//! `pg_stat_statements` in `shared_preload_libraries`. That is a server-level
//! setting, and a change needs a restart. So neither this command nor the
//! harness can provision it at connection time. The role in the URL must also
//! be allowed to call `pg_stat_statements_reset(...)`, as a superuser or by an
//! explicit GRANT.
//!
//! Neither requirement is checked up front. A vanilla external Postgres that
//! meets every other precondition completes the `EXPLAIN` sweep. It then fails
//! part way through, when the capture reaches its `pg_stat_statements`
//! snapshot. The testcontainer fallback does not have this problem. The image
//! and setup of `db::setup_bench_db()` in this crate provision the extension
//! itself. The review of PR #1339 reported this gap. The same gap exists in
//! `cargo xtask perf capability-labels-claim` and
//! `cargo xtask perf concurrency-key-claim`, and this command does not fix it
//! there.

use std::process::ExitCode;

use smop::prelude::*;

use super::broken_session_scan::support::{self, Capture, Ctx, Verdict};

/// The capture test that this command runs once.
const CAPTURE: Capture = Capture {
    filter: "claim_budget_tests::zz_capture_schedule_to_close_claim_evidence",
    log: "/tmp/schedule_to_close_claim_capture.log",
    out_dir: "docs/perf-artifacts/schedule-to-close-claim-predicate",
    before_after: false,
    marker: "^== capture complete: schedule-to-close evidence, artifacts in ",
    verdict: Verdict::Completion,
};

/// Arguments of `cargo xtask perf schedule-to-close-claim`.
#[derive(Debug, Args)]
pub struct Args {}

/// Runs `cargo xtask perf schedule-to-close-claim`.
pub fn run(_args: Args) -> Result<ExitCode> {
    support::run_capture(&Ctx::from_env(), &CAPTURE)
}
