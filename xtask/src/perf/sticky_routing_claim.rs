//! `cargo xtask perf sticky-routing-claim`: the port of
//! `autumn-harvest/scripts/sticky_routing_claim_perf_repro.sh`.
//!
//! The reproduction harness for the ordinary sticky-routing claim-predicate
//! measurement (issue #235, Ledger perf pass).
//! `docs/performance-sticky-routing.md` documents it.
//!
//! Like `cargo xtask perf worker-session-claim`, this command does NOT toggle a
//! code fix. `queue::claim_task_query()` is unmodified end to end. The command
//! runs the `#[ignore]`d evidence-capture test
//! `claim_budget_tests::zz_capture_sticky_routing_claim_evidence` ONCE.
//!
//! That single test drives the real `claim_task_query()` and `claim_task()` at
//! TWO seeded data states of `harvest_task_queue.sticky_worker_id`,
//! `sticky_until` and `sticky_timeout`. In one state all three are `NULL`, the
//! default today. In the other, every row is pinned to the claiming worker with
//! a future `sticky_until`. The test captures both, so this command has nothing
//! else to toggle.
//!
//! `session_id` stays `NULL` on both labels, unlike the worker-session capture.
//! That isolates ordinary sticky routing from the worker-sessions predicate.
//! That predicate sits immediately below it in the same `candidate` CTE.
//!
//! Usage, with or without `HARVEST_TEST_DATABASE_URL`:
//!
//! ```text
//! HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo xtask perf sticky-routing-claim
//!
//! # or, with only a reachable Docker daemon and no external Postgres:
//! cargo xtask perf sticky-routing-claim
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
//! It writes into `docs/perf-artifacts/sticky-routing-claim-predicate/`:
//!
//! - `{no-sticky,sticky-routing}-claim-backlog-{1000,10000,100000}.explain.txt`:
//!   `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)` for
//!   `claim_task_query()` at each published `BACKLOG_SWEEP` depth, at both
//!   seeded states.
//! - `{no-sticky,sticky-routing}-pg_stat_statements.txt`: a
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
//! An external Postgres instance also needs `pg_stat_statements` in
//! `shared_preload_libraries`. That is a server-level setting, and a change
//! needs a restart. The role in the URL must be allowed to call
//! `pg_stat_statements_reset(...)` and `SET pg_stat_statements.track = 'all'`,
//! as a superuser or by an explicit GRANT. The doc comment of
//! `zz_capture_worker_session_claim_evidence` gives the exact requirement.

use std::process::ExitCode;

use smop::prelude::*;

use super::support::{self, Capture, Ctx, Verdict};

/// The capture test that this command runs once.
const CAPTURE: Capture = Capture {
    filter: "claim_budget_tests::zz_capture_sticky_routing_claim_evidence",
    log: "/tmp/sticky_routing_claim_capture.log",
    out_dir: "docs/perf-artifacts/sticky-routing-claim-predicate",
    before_after: false,
    marker: "^== capture complete: sticky-routing evidence, artifacts in ",
    verdict: Verdict::Completion,
};

/// Arguments of `cargo xtask perf sticky-routing-claim`.
#[derive(Debug, Args)]
pub struct Args {}

/// Runs `cargo xtask perf sticky-routing-claim`.
pub fn run(_args: Args) -> Result<ExitCode> {
    support::run_capture(&Ctx::from_env(), &CAPTURE)
}
