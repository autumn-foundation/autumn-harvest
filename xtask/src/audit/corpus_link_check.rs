//! `cargo xtask audit corpus-link-check`: the port of `docs/audits/corpus-link-check.py`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask audit corpus-link-check`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask audit corpus-link-check`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask audit corpus-link-check` is not ported yet: {:?}",
        args.rest
    )
}
