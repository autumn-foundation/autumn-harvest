//! `cargo xtask ci chaos-watchdog`: the port of `.github/ci/chaos-watchdog.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask ci chaos-watchdog`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask ci chaos-watchdog`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask ci chaos-watchdog` is not ported yet: {:?}",
        args.rest
    )
}
