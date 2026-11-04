//! `cargo xtask bench notify-commit`: the port of `benchmarks/notify-commit.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask bench notify-commit`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask bench notify-commit`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask bench notify-commit` is not ported yet: {:?}",
        args.rest
    )
}
