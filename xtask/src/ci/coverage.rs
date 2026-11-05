//! `cargo xtask ci coverage`: the port of `.github/ci/coverage.py`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask ci coverage`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask ci coverage`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask ci coverage` is not ported yet: {:?}",
        args.rest
    )
}
