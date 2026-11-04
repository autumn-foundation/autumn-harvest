//! `cargo xtask ci run-suites`: the port of `.github/ci/run-suites.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask ci run-suites`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask ci run-suites`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask ci run-suites` is not ported yet: {:?}",
        args.rest
    )
}
