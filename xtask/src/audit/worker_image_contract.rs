//! `cargo xtask audit worker-image-contract`: the port of `docs/audits/worker-image-contract.py`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask audit worker-image-contract`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask audit worker-image-contract`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask audit worker-image-contract` is not ported yet: {:?}",
        args.rest
    )
}
