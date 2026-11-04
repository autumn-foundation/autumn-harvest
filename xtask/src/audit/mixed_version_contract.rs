//! `cargo xtask audit mixed-version-contract`: the port of `docs/audits/mixed-version-contract.py`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask audit mixed-version-contract`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask audit mixed-version-contract`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask audit mixed-version-contract` is not ported yet: {:?}",
        args.rest
    )
}
