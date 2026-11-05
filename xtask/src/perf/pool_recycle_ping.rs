//! `cargo xtask perf pool-recycle-ping`: the port of `autumn-harvest/scripts/pool_recycle_ping_perf_repro.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask perf pool-recycle-ping`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask perf pool-recycle-ping`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask perf pool-recycle-ping` is not ported yet: {:?}",
        args.rest
    )
}
