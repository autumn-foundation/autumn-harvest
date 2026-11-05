//! `cargo xtask dev flake-rerun`: the port of `scripts/flake-rerun.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask dev flake-rerun`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask dev flake-rerun`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask dev flake-rerun` is not ported yet: {:?}",
        args.rest
    )
}
