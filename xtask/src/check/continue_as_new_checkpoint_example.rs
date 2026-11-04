//! `cargo xtask check continue-as-new-checkpoint-example`: the port of `scripts/check-continue-as-new-checkpoint-example.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask check continue-as-new-checkpoint-example`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask check continue-as-new-checkpoint-example`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask check continue-as-new-checkpoint-example` is not ported yet: {:?}",
        args.rest
    )
}
