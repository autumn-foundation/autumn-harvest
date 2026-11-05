//! `cargo xtask bench run`: the port of `benchmarks/run.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask bench run`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask bench run`.
pub fn run(args: Args) -> Result<ExitCode> {
    let Args { rest } = args;
    bail!("`cargo xtask bench run` is not ported yet: {rest:?}")
}
