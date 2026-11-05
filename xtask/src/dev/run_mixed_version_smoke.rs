//! `cargo xtask dev run-mixed-version-smoke`: the port of `scripts/run-mixed-version-smoke.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask dev run-mixed-version-smoke`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask dev run-mixed-version-smoke`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask dev run-mixed-version-smoke` is not ported yet: {:?}",
        args.rest
    )
}
