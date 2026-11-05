//! Benchmark drivers (formerly `benchmarks/*.sh`).

use std::process::ExitCode;

use smop::prelude::*;

pub mod notify_commit;
pub mod run;

/// Each subcommand of `cargo xtask bench`.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Port of `benchmarks/run.sh`.
    Run(run::Args),
    /// Port of `benchmarks/notify-commit.sh`.
    NotifyCommit(notify_commit::Args),
}

/// Runs one subcommand of `cargo xtask bench`.
pub fn run(command: Command) -> Result<ExitCode> {
    match command {
        Command::Run(args) => run::run(args),
        Command::NotifyCommit(args) => notify_commit::run(args),
    }
}
