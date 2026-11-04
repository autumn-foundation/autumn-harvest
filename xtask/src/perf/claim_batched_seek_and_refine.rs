//! `cargo xtask perf claim-batched-seek-and-refine`: the port of `autumn-harvest/scripts/claim_batched_seek_and_refine_perf_repro.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask perf claim-batched-seek-and-refine`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask perf claim-batched-seek-and-refine`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask perf claim-batched-seek-and-refine` is not ported yet: {:?}",
        args.rest
    )
}
