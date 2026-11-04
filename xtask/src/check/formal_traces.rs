//! `cargo xtask check formal-traces`: the port of `scripts/check-formal-traces.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask check formal-traces`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask check formal-traces`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask check formal-traces` is not ported yet: {:?}",
        args.rest
    )
}
