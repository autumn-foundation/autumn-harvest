//! `cargo xtask check license-files`: the port of `scripts/check-license-files.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask check license-files`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask check license-files`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask check license-files` is not ported yet: {:?}",
        args.rest
    )
}
