//! CI drivers (formerly `.github/ci/*`).

use std::process::ExitCode;

use smop::prelude::*;

pub mod advisory_scan;
pub mod chaos_watchdog;
pub mod coverage;
pub mod pull_test_images;
pub mod run_suites;

/// Each subcommand of `cargo xtask ci`.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Port of `.github/ci/advisory-scan.sh`.
    AdvisoryScan(advisory_scan::Args),
    /// Port of `.github/ci/chaos-watchdog.sh`.
    ChaosWatchdog(chaos_watchdog::Args),
    /// Port of `.github/ci/coverage.py`.
    Coverage(coverage::Args),
    /// Port of `.github/ci/pull-test-images.sh`.
    PullTestImages(pull_test_images::Args),
    /// Port of `.github/ci/run-suites.sh`.
    RunSuites(run_suites::Args),
}

/// Runs one subcommand of `cargo xtask ci`.
pub fn run(command: Command) -> Result<ExitCode> {
    match command {
        Command::AdvisoryScan(args) => advisory_scan::run(args),
        Command::ChaosWatchdog(args) => chaos_watchdog::run(args),
        Command::Coverage(args) => coverage::run(args),
        Command::PullTestImages(args) => pull_test_images::run(args),
        Command::RunSuites(args) => run_suites::run(args),
    }
}
