//! `cargo xtask dev fuzz-smoke`: the port of `fuzz/smoke.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask dev fuzz-smoke`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask dev fuzz-smoke`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask dev fuzz-smoke` is not ported yet: {:?}",
        args.rest
    )
}
