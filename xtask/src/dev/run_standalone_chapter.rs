//! `cargo xtask dev run-standalone-chapter`: the port of `scripts/run-standalone-chapter.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask dev run-standalone-chapter`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask dev run-standalone-chapter`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask dev run-standalone-chapter` is not ported yet: {:?}",
        args.rest
    )
}
