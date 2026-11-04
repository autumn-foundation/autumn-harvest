//! `cargo xtask check default-run`: the port of `scripts/check-default-run.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask check default-run`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask check default-run`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask check default-run` is not ported yet: {:?}",
        args.rest
    )
}
