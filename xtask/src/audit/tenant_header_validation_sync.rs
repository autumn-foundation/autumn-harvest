//! `cargo xtask audit tenant-header-validation-sync`: the port of `docs/audits/tenant-header-validation-sync.py`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask audit tenant-header-validation-sync`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask audit tenant-header-validation-sync`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask audit tenant-header-validation-sync` is not ported yet: {:?}",
        args.rest
    )
}
