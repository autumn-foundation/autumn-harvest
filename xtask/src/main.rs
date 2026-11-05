//! `cargo xtask`: the workspace automation entry point.
//!
//! Every CI gate, doc guard and perf evidence generator of this repository
//! runs through this binary. The alias in `.cargo/config.toml` maps
//! `cargo xtask <group> <command>` to `cargo run -p xtask -- <group> <command>`.
//! `docs/xtask.md` lists each command and the script it replaced.

use std::process::ExitCode;

use smop::prelude::*;

mod audit;
mod bench;
mod check;
mod ci;
mod dev;
mod interrupt;
mod perf;
mod sandbox;
mod util;

/// Workspace automation for autumn-harvest.
#[derive(Debug, Parser)]
#[command(name = "xtask", about)]
struct Cli {
    #[command(subcommand)]
    group: Group,
}

/// The command groups of `cargo xtask`.
#[derive(Debug, Subcommand)]
enum Group {
    /// Docs and code audits that gate CI.
    #[command(subcommand)]
    Audit(audit::Command),
    /// Benchmark drivers.
    #[command(subcommand)]
    Bench(bench::Command),
    /// Example and documentation guards.
    #[command(subcommand)]
    Check(check::Command),
    /// CI drivers.
    #[command(subcommand)]
    Ci(ci::Command),
    /// Developer and release tooling.
    #[command(subcommand)]
    Dev(dev::Command),
    /// Performance evidence generators.
    #[command(subcommand)]
    Perf(perf::Command),
    /// Sandbox guest image tooling.
    #[command(subcommand)]
    Sandbox(sandbox::Command),
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    // The perf and bench commands hold cleanup guards for their whole run:
    // containers, scratch databases and moved source files. So a signal must
    // unwind them, not kill them. See `perf::support::stop_on_interrupt`.
    let guard = matches!(cli.group, Group::Bench(_) | Group::Perf(_)).then(interrupt::install);
    let outcome = match cli.group {
        Group::Audit(command) => audit::run(command),
        Group::Bench(command) => bench::run(command),
        Group::Check(command) => check::run(command),
        Group::Ci(command) => ci::run(command),
        Group::Dev(command) => dev::run(command),
        Group::Perf(command) => perf::run(command),
        Group::Sandbox(command) => sandbox::run(command),
    };
    if guard.is_some() && interrupt::requested() {
        eprintln!("interrupted");
        return ExitCode::from(interrupt::EXIT_CODE);
    }
    drop(guard);
    match outcome {
        Ok(code) => code,
        Err(error) => {
            eprintln!("error: {error:#}");
            ExitCode::from(2)
        }
    }
}
