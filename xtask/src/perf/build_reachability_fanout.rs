//! `cargo xtask perf build-reachability-fanout`: the port of `autumn-harvest/scripts/build_reachability_fanout_perf_repro.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask perf build-reachability-fanout`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask perf build-reachability-fanout`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask perf build-reachability-fanout` is not ported yet: {:?}",
        args.rest
    )
}
