//! `cargo xtask audit libpq-keyword-sync`: the port of `docs/audits/libpq-keyword-sync.py`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask audit libpq-keyword-sync`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask audit libpq-keyword-sync`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask audit libpq-keyword-sync` is not ported yet: {:?}",
        args.rest
    )
}
