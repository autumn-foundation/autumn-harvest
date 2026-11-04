//! `cargo xtask audit vantage-degraded-banner-focus`: the port of `docs/audits/vantage-degraded-banner-focus.py`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask audit vantage-degraded-banner-focus`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask audit vantage-degraded-banner-focus`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask audit vantage-degraded-banner-focus` is not ported yet: {:?}",
        args.rest
    )
}
