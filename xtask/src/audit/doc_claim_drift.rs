//! `cargo xtask audit doc-claim-drift`: the port of `docs/audits/doc-claim-drift.py`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask audit doc-claim-drift`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask audit doc-claim-drift`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask audit doc-claim-drift` is not ported yet: {:?}",
        args.rest
    )
}
