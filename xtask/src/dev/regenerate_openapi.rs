//! `cargo xtask dev regenerate-openapi`: the port of `scripts/regenerate-openapi.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask dev regenerate-openapi`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask dev regenerate-openapi`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask dev regenerate-openapi` is not ported yet: {:?}",
        args.rest
    )
}
