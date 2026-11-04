//! `cargo xtask perf history-bloat`: the port of `autumn-harvest-plugin/scripts/history_bloat_perf_repro.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask perf history-bloat`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask perf history-bloat`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask perf history-bloat` is not ported yet: {:?}",
        args.rest
    )
}
