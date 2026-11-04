//! `cargo xtask perf pause-array-size-claim`: the port of `autumn-harvest/scripts/pause_array_size_claim_perf_repro.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask perf pause-array-size-claim`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask perf pause-array-size-claim`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask perf pause-array-size-claim` is not ported yet: {:?}",
        args.rest
    )
}
