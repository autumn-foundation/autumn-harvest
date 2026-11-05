//! `cargo xtask perf external-outbox-scan`: the port of
//! `autumn-harvest/scripts/external_outbox_scan_perf_repro.sh`.
//!
//! The reproduction harness for the external-outbox scan indexes and the
//! paired claim-query rewrite (issue #1486).
//! `docs/performance-external-outbox-scan.md` documents it.
//!
//! The evidence-capture test
//! `external_outbox_scan_tests::zz_capture_external_outbox_scan_evidence`
//! seeds the fixture of the issue once. It then captures two full drains of
//! the same outbox against it.
//!
//! - "before": the pre-fix claim query, with the four candidate indexes
//!   dropped. The test drops them unconditionally. So it reproduces the
//!   baseline whether or not
//!   `20260911213344_harvest_external_outbox_scan_indexes` has already run
//!   against the target database.
//! - "after": the rewritten claim query, with the four indexes built from the
//!   own SQL of that migration.
//!
//! Both drains resolve the same request set, and the test asserts that. A
//! capture that changed which rows the scanner claims fails. It does not
//! publish a number.
//!
//! Usage, with or without `HARVEST_TEST_DATABASE_URL`:
//!
//! ```text
//! HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo xtask perf external-outbox-scan
//!
//! # or, with only a reachable Docker daemon and no external Postgres:
//! cargo xtask perf external-outbox-scan
//! ```
//!
//! `HARVEST_TEST_DATABASE_URL`, when set, is an ADMIN URL.
//! `claim_bench_support.rs` treats it the same way elsewhere in this crate.
//! The harness creates, migrates, seeds, measures and drops a fresh, uniquely
//! named database per run. When the variable is unset,
//! `claim_bench_support::db::setup_bench_db` falls back to a testcontainer.
//! With neither an external database nor a Docker daemon, the capture test
//! skips loudly and writes no artifacts.
//!
//! It writes into `docs/perf-artifacts/external-outbox-scan/`:
//!
//! - `{before,after}.explain.txt`:
//!   `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)` for one cold
//!   claim of each form.
//! - `{before,after}.pg_stat_statements.txt`: a `pg_stat_statements` snapshot
//!   that covers the whole drain of each form. It is scoped to the own
//!   database of this run.
//!
//! Preconditions: a Rust toolchain that can build this crate, and either
//! Docker or a reachable Postgres that `HARVEST_TEST_DATABASE_URL` names. That
//! server needs `pg_stat_statements` in `shared_preload_libraries`. The
//! harness creates the extension itself. The C hooks exist only once the
//! library is preloaded at postmaster start. A server without it reports
//! zero rows.

use std::process::ExitCode;

use smop::prelude::*;

use super::broken_session_scan::support::{self, Ctx};

/// The `cargo test` arguments of the capture, as the script passed them to `exec`.
const CAPTURE_ARGS: [&str; 11] = [
    "test",
    "-p",
    "autumn-harvest",
    "--features",
    "db",
    "--test",
    "integration",
    "zz_capture_external_outbox_scan_evidence",
    "--",
    "--ignored",
    "--nocapture",
];

/// Arguments of `cargo xtask perf external-outbox-scan`.
#[derive(Debug, Args)]
pub struct Args {}

/// Runs `cargo xtask perf external-outbox-scan`.
pub fn run(_args: Args) -> Result<ExitCode> {
    let ctx = Ctx::from_env();
    let status = support::status(&mut ctx.cargo(CAPTURE_ARGS))?;
    Ok(support::exit_code(status))
}
