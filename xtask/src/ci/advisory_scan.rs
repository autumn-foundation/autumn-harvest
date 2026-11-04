//! `cargo xtask ci advisory-scan`: the port of `.github/ci/advisory-scan.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask ci advisory-scan`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask ci advisory-scan`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask ci advisory-scan` is not ported yet: {:?}",
        args.rest
    )
}
