//! `cargo xtask audit ledger-profiles`: the port of `docs/audits/ledger-profiles.py`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask audit ledger-profiles`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask audit ledger-profiles`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask audit ledger-profiles` is not ported yet: {:?}",
        args.rest
    )
}
