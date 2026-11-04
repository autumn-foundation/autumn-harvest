//! `cargo xtask sandbox publish-image`: the port of `scripts/sandbox/publish-image.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask sandbox publish-image`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask sandbox publish-image`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask sandbox publish-image` is not ported yet: {:?}",
        args.rest
    )
}
