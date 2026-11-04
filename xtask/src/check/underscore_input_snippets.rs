//! `cargo xtask check underscore-input-snippets`: the port of `scripts/check-underscore-input-snippets.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask check underscore-input-snippets`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask check underscore-input-snippets`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask check underscore-input-snippets` is not ported yet: {:?}",
        args.rest
    )
}
