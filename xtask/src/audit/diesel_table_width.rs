//! `cargo xtask audit diesel-table-width`: the port of `docs/audits/diesel-table-width.py`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask audit diesel-table-width`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask audit diesel-table-width`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask audit diesel-table-width` is not ported yet: {:?}",
        args.rest
    )
}
