//! `cargo xtask dev build-typescript-client`: the port of `scripts/build-typescript-client.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask dev build-typescript-client`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask dev build-typescript-client`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask dev build-typescript-client` is not ported yet: {:?}",
        args.rest
    )
}
