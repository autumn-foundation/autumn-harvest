//! `cargo xtask dev clean-room-getting-started`: the port of `scripts/clean-room-getting-started.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask dev clean-room-getting-started`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask dev clean-room-getting-started`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask dev clean-room-getting-started` is not ported yet: {:?}",
        args.rest
    )
}
