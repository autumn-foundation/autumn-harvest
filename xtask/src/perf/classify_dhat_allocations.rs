//! `cargo xtask perf classify-dhat-allocations`: the port of `autumn-harvest-sqlite/scripts/classify_dhat_allocations.py`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask perf classify-dhat-allocations`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask perf classify-dhat-allocations`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask perf classify-dhat-allocations` is not ported yet: {:?}",
        args.rest
    )
}
