//! `cargo xtask perf deep-backlog-ledger`: the port of `autumn-harvest/scripts/deep_backlog_ledger_repro.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask perf deep-backlog-ledger`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask perf deep-backlog-ledger`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask perf deep-backlog-ledger` is not ported yet: {:?}",
        args.rest
    )
}
