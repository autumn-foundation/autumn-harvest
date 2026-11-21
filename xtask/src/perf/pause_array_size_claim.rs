//! `cargo xtask perf pause-array-size-claim`: the port of
//! `autumn-harvest/scripts/pause_array_size_claim_perf_repro.sh`.
//!
//! The reproduction harness for the pause-array-size finding (issue #1215).
//! `docs/performance.md` documents it.
//!
//! Like `cargo xtask perf capability-labels-claim`, this command does NOT
//! toggle a code fix. `queue::claim_task_query()` is unmodified end to end.
//! Issue #1215 found a cost that scales with the width of the pause array, not
//! a query-shape defect to rewrite. The command runs the `#[ignore]`d
//! evidence-capture test
//! `claim_budget_tests::zz_capture_pause_array_size_claim_evidence` ONCE.
//!
//! The test sweeps the array size over 0, 1, 20 and 199 ballast pause rows.
//! The ballast has 0% selectivity, so a change in which rows are eligible
//! cannot explain the effect. The sweep covers three shapes. The first is
//! `paused_activities` (#807), which is unconditional. The other two are
//! `paused_queues` (#619). One binds it to the own queues of a typical
//! worker. The other binds it to an atypical worker whose own bind is itself
//! wide.
//!
//! Usage, with or without `HARVEST_TEST_DATABASE_URL`:
//!
//! ```text
//! HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo xtask perf pause-array-size-claim
//!
//! # or, with only a reachable Docker daemon and no external Postgres:
//! cargo xtask perf pause-array-size-claim
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
//! It writes into `docs/perf-artifacts/pause-array-size/`:
//!
//! - `activity-pause-array-{0,1,20,199}.explain.txt`,
//!   `queue-pause-bound-array-{0,1,20,199}.explain.txt` and
//!   `queue-pause-wide-array-{0,199}.explain.txt`:
//!   `EXPLAIN (ANALYZE, BUFFERS, SETTINGS, TIMING OFF)` for `claim_task_query()`
//!   at a fixed 10,000-row backlog. There is one file per array size per shape.
//! - `summary.txt`: one line per capture. Each line holds the predicate, the
//!   array size, the backlog, and the `Sort Method:` line of the plan, or
//!   "(no Sort node)".
//!
//! Preconditions: a Rust toolchain that can build this crate. The command also needs Docker
//! for the testcontainer fallback of the harness, or a reachable Postgres that
//! `HARVEST_TEST_DATABASE_URL` names.

use std::process::ExitCode;

use smop::prelude::*;

use super::support::{self, Capture, Ctx, Verdict};

/// The capture test that this command runs once.
const CAPTURE: Capture = Capture {
    filter: "claim_budget_tests::zz_capture_pause_array_size_claim_evidence",
    log: "/tmp/pause_array_size_claim_capture.log",
    out_dir: "docs/perf-artifacts/pause-array-size",
    before_after: false,
    marker: "^== capture complete: artifacts in ",
    verdict: Verdict::Completion,
};

/// Arguments of `cargo xtask perf pause-array-size-claim`.
#[derive(Debug, Args)]
pub struct Args {}

/// Runs `cargo xtask perf pause-array-size-claim`.
pub fn run(_args: Args) -> Result<ExitCode> {
    support::run_capture(&Ctx::from_env(), &CAPTURE)
}
