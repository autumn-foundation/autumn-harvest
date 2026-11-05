//! `cargo xtask check no-powershell-env-syntax`: the port of `scripts/check-no-powershell-env-syntax.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask check no-powershell-env-syntax`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask check no-powershell-env-syntax`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask check no-powershell-env-syntax` is not ported yet: {:?}",
        args.rest
    )
}
