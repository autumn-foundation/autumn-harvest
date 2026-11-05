//! `cargo xtask audit tracked-artifacts`: the port of `docs/audits/tracked-artifacts.py`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask audit tracked-artifacts`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask audit tracked-artifacts`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask audit tracked-artifacts` is not ported yet: {:?}",
        args.rest
    )
}
