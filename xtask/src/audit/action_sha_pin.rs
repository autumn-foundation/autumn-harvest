//! `cargo xtask audit action-sha-pin`: the port of `docs/audits/action-sha-pin.py`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask audit action-sha-pin`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask audit action-sha-pin`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask audit action-sha-pin` is not ported yet: {:?}",
        args.rest
    )
}
