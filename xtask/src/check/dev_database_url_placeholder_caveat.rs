//! `cargo xtask check dev-database-url-placeholder-caveat`: the port of `scripts/check-dev-database-url-placeholder-caveat.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask check dev-database-url-placeholder-caveat`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask check dev-database-url-placeholder-caveat`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask check dev-database-url-placeholder-caveat` is not ported yet: {:?}",
        args.rest
    )
}
