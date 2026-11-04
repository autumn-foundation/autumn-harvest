//! `cargo xtask sandbox image-path`: the port of `scripts/sandbox/image-path.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask sandbox image-path`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask sandbox image-path`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask sandbox image-path` is not ported yet: {:?}",
        args.rest
    )
}
