//! `cargo xtask audit root-files`: the port of `docs/audits/root-files.py`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask audit root-files`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask audit root-files`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask audit root-files` is not ported yet: {:?}",
        args.rest
    )
}
