//! `cargo xtask perf parent-close-cascade-unfinished-handlers`: the port of
//! `autumn-harvest/scripts/parent_close_cascade_unfinished_handlers_perf_repro.sh`.
//!
//! The reproduction harness for the N+1 fix in the unfinished-update-handler
//! check of the parent-close cascade (Ledger perf pass).
//! `docs/performance-parent-close-cascade-unfinished-handlers.md` documents it.
//!
//! Before this fix, each caller of the `closed_children` of
//! `apply_parent_close_cascade` looped over its collected
//! `(exec_id, workflow_name)` pairs. There were five such sites in `worker.rs`
//! and three more in `timeout.rs`. The admission-path `deferred_checks` shape
//! in `execution.rs` and `completion_trigger.rs` did the same. Each loop called
//! `check_and_report_unfinished_handlers` once per pair. That issued one
//! `SELECT * FROM harvest_events WHERE workflow_exec_id = $1` per pair, after
//! the writing transaction committed.
//!
//! This command drives BOTH strategies against the SAME seeded fixture. The
//! old per-pair loop is reproduced in the test itself, and it calls the
//! single-execution function that is still present. The new strategy is
//! `check_and_report_unfinished_handlers_batch`. The test captures a
//! `pg_stat_statements` snapshot for each.
//!
//! Usage, with or without `HARVEST_TEST_DATABASE_URL`:
//!
//! ```text
//! HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo xtask perf parent-close-cascade-unfinished-handlers
//!
//! # or, with only a reachable Docker daemon and no external Postgres:
//! cargo xtask perf parent-close-cascade-unfinished-handlers
//! ```
//!
//! `HARVEST_TEST_DATABASE_URL`, when set, is an ADMIN URL.
//! `claim_bench_support.rs` treats it the same way elsewhere in this crate.
//! `claim_bench_support::db::setup_bench_db` creates, migrates and measures
//! against a fresh, uniquely named database. It holds an idle lease connection
//! open instead of dropping it, as the doc comment of that function explains.
//! When the variable is unset, it falls back to a testcontainer. The daemon
//! reclaims that container entirely on its own.
//!
//! It writes into `docs/perf-artifacts/parent-close-cascade-unfinished-handlers/`:
//!
//! - `before.pg_stat_statements.txt` and `before.result-rows.txt`: the looped
//!   per-child `check_and_report_unfinished_handlers` strategy. The first holds
//!   `pg_stat_statements` for the per-exec-id `harvest_events` SELECT. The
//!   second holds every reported `(workflow_name, count)` pair.
//! - `after.pg_stat_statements.txt` and `after.result-rows.txt`: the real,
//!   shipped `check_and_report_unfinished_handlers_batch`. It issues one
//!   `eq_any` `harvest_events` SELECT and reports the same pairs.
//!
//! Preconditions: a Rust toolchain that can build this crate. The command
//! also needs Docker for the testcontainer fallback of the harness, or a
//! reachable Postgres that `HARVEST_TEST_DATABASE_URL` names. That server
//! needs `pg_stat_statements` in `shared_preload_libraries`.

use std::process::ExitCode;

use smop::prelude::*;

use super::support::{self, Capture, Ctx, Verdict};

/// The capture test that this command runs once.
const CAPTURE: Capture = Capture {
    filter: "parent_close_cascade_unfinished_handlers_perf::zz_capture_parent_close_cascade_unfinished_handlers_evidence",
    log: "/tmp/parent_close_cascade_unfinished_handlers_capture.log",
    out_dir: "docs/perf-artifacts/parent-close-cascade-unfinished-handlers",
    before_after: false,
    marker: "^== done. Artifacts in ",
    verdict: Verdict::Completion,
};

/// Arguments of `cargo xtask perf parent-close-cascade-unfinished-handlers`.
#[derive(Debug, Args)]
pub struct Args {}

/// Runs `cargo xtask perf parent-close-cascade-unfinished-handlers`.
pub fn run(_args: Args) -> Result<ExitCode> {
    support::run_capture(&Ctx::from_env(), &CAPTURE)
}
