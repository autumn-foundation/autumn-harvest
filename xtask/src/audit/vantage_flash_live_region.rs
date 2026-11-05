//! `cargo xtask audit vantage-flash-live-region`: the port of `docs/audits/vantage-flash-live-region.py`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask audit vantage-flash-live-region`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask audit vantage-flash-live-region`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask audit vantage-flash-live-region` is not ported yet: {:?}",
        args.rest
    )
}
